//! The thread-safe cache and its policy view.

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use crate::builder::CacheBuilder;
use crate::config::{Config, Loader};
use crate::removal::{RemovalCause, RemovedEntry};
use crate::shard::Shard;
use crate::stats::{CacheStats, StatsCounters};

/// A high-performance in-memory cache with W-TinyLFU eviction.
///
/// The cache is a cheap, cloneable handle backed by shared state. All methods
/// take `&self`; clone the handle to share it between threads.
///
/// Keys must be `Clone` because the eviction policy keeps its own copy for
/// list ordering. Values are returned by clone; store an `Arc` in the cache
/// when cloning is expensive.
pub struct Cache<K, V> {
    inner: Arc<Inner<K, V>>,
}

pub(crate) struct Inner<K, V> {
    pub(crate) config: Config<K, V>,
    pub(crate) shards: Box<[Mutex<Shard<K, V>>]>,
    pub(crate) shard_mask: usize,
    pub(crate) hasher: RandomState,
    pub(crate) stats: StatsCounters,
    loads: Mutex<HashMap<K, Arc<LoadSlot>>>,
}

/// A load in progress, used to coalesce concurrent misses on one key.
struct LoadSlot {
    owner: ThreadId,
    done: Mutex<bool>,
    ready: Condvar,
}

struct LoadGuard<'a, K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    cache: &'a Cache<K, V>,
    key: K,
}

impl<K, V> Drop for LoadGuard<'_, K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.cache.finish_load(&self.key);
    }
}

impl<K, V> Clone for Cache<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V> Cache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Returns a new [`CacheBuilder`].
    pub fn builder() -> CacheBuilder<K, V> {
        CacheBuilder::new()
    }

    pub(crate) fn new(config: Config<K, V>) -> Self {
        let shard_count = shard_count(config.max_capacity);
        let capacities = distribute_capacity(config.max_capacity, shard_count);
        let initial_per_shard = (config.initial_capacity / shard_count).max(1);
        let max_capacity = config.max_capacity;
        let expiration = config.expiration.clone();
        let bounded = config.bounded;
        let shards: Vec<Mutex<Shard<K, V>>> = capacities
            .into_iter()
            .map(|capacity| {
                Mutex::new(Shard::new(
                    capacity,
                    max_capacity,
                    initial_per_shard,
                    expiration.clone(),
                    bounded,
                ))
            })
            .collect();
        debug_assert!(shard_count.is_power_of_two());
        Self {
            inner: Arc::new(Inner {
                config,
                shards: shards.into_boxed_slice(),
                shard_mask: shard_count - 1,
                hasher: RandomState::new(),
                stats: StatsCounters::default(),
                loads: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Returns the value for `key`, recording a hit or miss.
    ///
    /// Looking up a stale entry may schedule an asynchronous refresh and
    /// still returns the stale value.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        let hit = {
            let mut shard = lock(&self.inner.shards[shard_index]);
            shard.get(key, now, self.inner.config.refresh_ttl(), &mut removed)
        };
        self.notify(removed);
        match hit {
            Some(hit) => {
                self.record_hit();
                if let Some((refresh_key, write_time)) = hit.refresh {
                    self.submit_refresh(refresh_key, write_time);
                }
                Some(hit.value)
            }
            None => {
                self.record_miss();
                None
            }
        }
    }

    /// Returns the live entries for `keys`, recording hits and misses.
    ///
    /// This mirrors `Cache.getAllPresent`: absent keys are simply omitted
    /// from the result.
    pub fn get_all_present(&self, keys: impl IntoIterator<Item = K>) -> HashMap<K, V> {
        keys.into_iter()
            .filter_map(|key| self.get(&key).map(|value| (key, value)))
            .collect()
    }

    /// Returns the entries for `keys`, invoking `mapping` once for the keys
    /// that were missing.
    ///
    /// Every entry returned by `mapping` is stored, but only requested keys
    /// appear in the result. This mirrors `Cache.getAll`.
    pub fn get_all<F>(&self, keys: impl IntoIterator<Item = K>, mapping: F) -> HashMap<K, V>
    where
        F: FnOnce(&[K]) -> HashMap<K, V>,
    {
        let mut result = HashMap::new();
        let mut missing = Vec::new();
        for key in keys {
            match self.get(&key) {
                Some(value) => {
                    result.insert(key, value);
                }
                None => missing.push(key),
            }
        }
        if missing.is_empty() {
            return result;
        }

        let started = Instant::now();
        let loaded = mapping(&missing);
        for (key, value) in loaded {
            let requested = missing.contains(&key);
            self.insert(key.clone(), value.clone());
            self.record_load_success(started);
            if requested {
                result.insert(key, value);
            }
        }
        result
    }

    /// Returns the value for `key` without recording statistics, updating
    /// recency, or extending an access-based TTL.
    pub fn peek<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        let value = {
            let mut shard = lock(&self.inner.shards[shard_index]);
            shard.peek(key, now, &mut removed)
        };
        self.notify(removed);
        value
    }

    /// Returns `true` if a live entry exists, without affecting statistics or
    /// recency.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        let found = {
            let mut shard = lock(&self.inner.shards[shard_index]);
            shard.contains(key, now, &mut removed)
        };
        self.notify(removed);
        found
    }

    /// Returns the cached value, loading and inserting it on a miss.
    ///
    /// Concurrent misses for the same key are coalesced: one caller runs
    /// `init` while the others wait and reuse the cached value. If the loader
    /// panics, the waiters wake and retry. Recursively loading the same key
    /// from inside its initializer panics.
    pub fn get_with<F>(&self, key: K, init: F) -> V
    where
        F: FnOnce(&K) -> V,
    {
        if let Some(value) = self.get(&key) {
            return value;
        }
        let mut init = Some(init);
        loop {
            match self.begin_load(&key) {
                Some(slot) => {
                    self.wait_for_load(&slot);
                    // A non-recording read: the original get already counted
                    // this request as a miss.
                    if let Some(value) = self.peek(&key) {
                        return value;
                    }
                    // The loader failed, panicked, or produced a value that
                    // could not be cached. Try to load again.
                }
                None => {
                    let _guard = LoadGuard {
                        cache: self,
                        key: key.clone(),
                    };
                    if let Some(value) = self.peek(&key) {
                        return value;
                    }
                    let started = Instant::now();
                    let value = init.take().expect("the loader runs once")(&key);
                    self.record_load_success(started);
                    self.insert(key, value.clone());
                    return value;
                }
            }
        }
    }

    /// Like [`get_with`](Self::get_with), but the loader may fail.
    pub fn try_get_with<E, F>(&self, key: K, init: F) -> Result<V, E>
    where
        F: FnOnce(&K) -> Result<V, E>,
    {
        if let Some(value) = self.get(&key) {
            return Ok(value);
        }
        let mut init = Some(init);
        loop {
            match self.begin_load(&key) {
                Some(slot) => {
                    self.wait_for_load(&slot);
                    if let Some(value) = self.peek(&key) {
                        return Ok(value);
                    }
                }
                None => {
                    let _guard = LoadGuard {
                        cache: self,
                        key: key.clone(),
                    };
                    if let Some(value) = self.peek(&key) {
                        return Ok(value);
                    }
                    let started = Instant::now();
                    match init.take().expect("the loader runs once")(&key) {
                        Ok(value) => {
                            self.record_load_success(started);
                            self.insert(key, value.clone());
                            return Ok(value);
                        }
                        Err(error) => {
                            self.record_load_failure(started);
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    /// Loads the value for `key` with the loader configured by
    /// [`build_with_loader`](crate::CacheBuilder::build_with_loader).
    ///
    /// This mirrors `LoadingCache.get`. A loader returning `None` leaves the
    /// key absent. Concurrent misses for the same key are coalesced.
    ///
    /// # Panics
    ///
    /// Panics when no loader was configured. Use [`get`](Self::get) for caches
    /// without a loader.
    pub fn get_loaded(&self, key: K) -> Option<V> {
        let loader = self
            .inner
            .config
            .loader
            .clone()
            .expect("get_loaded requires a loader; build with build_with_loader");
        if let Some(value) = self.get(&key) {
            return Some(value);
        }
        loop {
            match self.begin_load(&key) {
                Some(slot) => {
                    self.wait_for_load(&slot);
                    if let Some(value) = self.peek(&key) {
                        return Some(value);
                    }
                }
                None => {
                    let _guard = LoadGuard {
                        cache: self,
                        key: key.clone(),
                    };
                    if let Some(value) = self.peek(&key) {
                        return Some(value);
                    }
                    let started = Instant::now();
                    let value = loader(&key)?;
                    self.record_load_success(started);
                    self.insert(key.clone(), value.clone());
                    return Some(value);
                }
            }
        }
    }

    /// Loads every missing key with the configured loader.
    ///
    /// This mirrors `LoadingCache.getAll`; the returned map contains only the
    /// keys that resolved to a value.
    pub fn get_all_loaded(&self, keys: impl IntoIterator<Item = K>) -> HashMap<K, V> {
        keys.into_iter()
            .filter_map(|key| self.get_loaded(key.clone()).map(|value| (key, value)))
            .collect()
    }

    // ------------------------------------------------------------------
    // Load coalescing
    // ------------------------------------------------------------------

    /// Registers a load for `key`, or returns the in-progress load to wait on.
    fn begin_load(&self, key: &K) -> Option<Arc<LoadSlot>> {
        let mut loads = lock(&self.inner.loads);
        if let Some(slot) = loads.get(key) {
            if slot.owner == std::thread::current().id() {
                panic!("recursive load of the same key");
            }
            Some(Arc::clone(slot))
        } else {
            loads.insert(
                key.clone(),
                Arc::new(LoadSlot {
                    owner: std::thread::current().id(),
                    done: Mutex::new(false),
                    ready: Condvar::new(),
                }),
            );
            None
        }
    }

    fn wait_for_load(&self, slot: &LoadSlot) {
        let mut done = lock(&slot.done);
        while !*done {
            done = slot
                .ready
                .wait(done)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn finish_load(&self, key: &K) {
        let slot = lock(&self.inner.loads).remove(key);
        if let Some(slot) = slot {
            *lock(&slot.done) = true;
            slot.ready.notify_all();
        }
    }

    /// Inserts or replaces an entry, notifying listeners for a replaced value.
    pub fn insert(&self, key: K, value: V) {
        let removed = self.insert_entry(key, value);
        self.notify(removed);
    }

    /// Inserts or replaces an entry and returns the previous value, matching
    /// `ConcurrentMap.put`.
    pub fn put_returning(&self, key: K, value: V) -> Option<V> {
        let removed = self.insert_entry(key, value);
        let previous = removed
            .iter()
            .find(|entry| entry.cause == RemovalCause::Replaced)
            .map(|entry| entry.value.clone());
        self.notify(removed);
        previous
    }

    fn insert_entry(&self, key: K, value: V) -> Vec<RemovedEntry<K, V>> {
        let now = self.now();
        let hash = self.hash_key(&key);
        let weight = self.inner.config.weight(&key, &value);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        {
            let mut shard = lock(&self.inner.shards[shard_index]);
            shard.insert(key, value, hash, weight, now, &mut removed);
        }
        removed
    }

    /// Alias for [`insert`](Self::insert), matching `ConcurrentMap::put`.
    pub fn put(&self, key: K, value: V) {
        self.insert(key, value);
    }

    /// Inserts every entry, mirroring `Cache.putAll`.
    pub fn put_all(&self, entries: impl IntoIterator<Item = (K, V)>) {
        for (key, value) in entries {
            self.insert(key, value);
        }
    }

    /// Inserts only when the key is absent, returning the existing value
    /// otherwise. The check and insert happen under one shard lock.
    pub fn put_if_absent(&self, key: K, value: V) -> Option<V> {
        let now = self.now();
        let hash = self.hash_key(&key);
        let weight = self.inner.config.weight(&key, &value);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        let previous = {
            let mut shard = lock(&self.inner.shards[shard_index]);
            match shard.peek(&key, now, &mut removed) {
                Some(existing) => Some(existing),
                None => {
                    shard.insert(key, value, hash, weight, now, &mut removed);
                    None
                }
            }
        };
        self.notify(removed);
        previous
    }

    /// Returns a live, map-shaped view over this cache, mirroring
    /// `Cache.asMap`.
    ///
    /// Reads and writes go through the cache itself. Iteration returns a
    /// snapshot, because holding the shard lock across a user loop is not
    /// possible in safe Rust.
    pub fn as_map(&self) -> CacheMap<'_, K, V> {
        CacheMap { cache: self }
    }

    /// Removes an entry, returning its value if it was present.
    pub fn invalidate<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.remove_with_cause(key, RemovalCause::Explicit)
    }

    pub(crate) fn remove_with_cause<Q>(&self, key: &Q, cause: RemovalCause) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        let value = {
            let mut shard = lock(&self.inner.shards[shard_index]);
            shard.remove(key, cause, now, &mut removed)
        };
        self.notify(removed);
        value
    }

    /// Alias for [`invalidate`](Self::invalidate).
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.invalidate(key)
    }

    /// Removes every entry.
    pub fn invalidate_all(&self) {
        let mut removed = Vec::new();
        for shard in self.inner.shards.iter() {
            removed.append(&mut lock(shard).drain());
        }
        self.notify(removed);
    }

    /// Removes every supplied key, mirroring `Cache.invalidateAll(Iterable)`.
    pub fn invalidate_keys<Q>(&self, keys: impl IntoIterator<Item = Q>)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        for key in keys {
            self.invalidate(&key);
        }
    }

    /// Alias for [`invalidate_all`](Self::invalidate_all).
    pub fn clear(&self) {
        self.invalidate_all();
    }

    /// Performs maintenance, removing every expired entry.
    pub fn clean_up(&self) {
        let now = self.now();
        let mut removed = Vec::new();
        for shard in self.inner.shards.iter() {
            lock(shard).clean_expired(now, &mut removed);
        }
        self.notify(removed);
    }

    /// Removes entries for which `predicate` returns `false`.
    pub fn retain<F>(&self, mut predicate: F)
    where
        F: FnMut(&K, &V) -> bool,
    {
        let entries: Vec<(K, V)> = self.iter().collect();
        for (key, value) in entries {
            if !predicate(&key, &value) {
                self.invalidate(&key);
            }
        }
    }

    /// Number of entries currently held, including not-yet-maintained expired
    /// entries.
    pub fn len(&self) -> usize {
        self.inner
            .shards
            .iter()
            .map(|shard| lock(shard).len())
            .sum()
    }

    /// Returns `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate number of entries, including not-yet-maintained expired
    /// entries.
    pub fn estimated_size(&self) -> usize {
        self.len()
    }

    /// Sum of the weights of all held entries.
    pub fn weighted_size(&self) -> u64 {
        self.inner
            .shards
            .iter()
            .map(|shard| lock(shard).weighted_size())
            .sum()
    }

    /// Clones every live entry into an iterator.
    ///
    /// Expired entries are removed first, exactly as a maintenance pass would.
    pub fn iter(&self) -> std::vec::IntoIter<(K, V)> {
        let now = self.now();
        let mut entries = Vec::new();
        let mut removed = Vec::new();
        for shard in self.inner.shards.iter() {
            entries.extend(lock(shard).snapshot(now, &mut removed));
        }
        self.notify(removed);
        entries.into_iter()
    }

    /// Returns a snapshot of the statistics counters.
    ///
    /// All counters are zero unless statistics collection was enabled.
    pub fn stats(&self) -> CacheStats {
        self.inner.stats.snapshot(self.inner.config.record_stats)
    }

    /// Returns a view of the cache's configuration and current policy size.
    pub fn policy(&self) -> Policy<'_, K, V> {
        Policy { cache: self }
    }

    /// Returns `true` when statistics collection is enabled.
    pub fn is_recording_stats(&self) -> bool {
        self.inner.config.record_stats
    }

    /// Returns the value without recording a hit or miss.
    pub fn peek_quietly<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.peek(key)
    }

    /// Clones every live key.
    pub fn keys(&self) -> Vec<K> {
        self.iter().map(|(key, _)| key).collect()
    }

    /// Returns the weight of an entry when the cache is weighted.
    pub fn weight_of<Q>(&self, key: &Q) -> Option<u32>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        lock(&self.inner.shards[shard_index]).weight_of(key, now)
    }

    /// Changes the maximum size or weight and eagerly evicts any excess.
    pub fn set_maximum(&self, maximum: u64) {
        let mut removed = Vec::new();
        for shard in self.inner.shards.iter() {
            lock(shard).set_maximum(maximum, &mut removed);
        }
        self.notify(removed);
    }

    /// Snapshot ordered from least to most likely to be retained.
    pub fn coldest(&self, limit: usize) -> Vec<(K, V)> {
        lock(&self.inner.shards[0]).coldest(limit)
    }

    /// Snapshot ordered from most to least likely to be retained.
    pub fn hottest(&self, limit: usize) -> Vec<(K, V)> {
        lock(&self.inner.shards[0]).hottest(limit)
    }

    /// Time since the entry's last access or write.
    pub fn age_of<Q>(&self, key: &Q, access: bool) -> Option<Duration>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        lock(&self.inner.shards[shard_index])
            .age_of(key, now, access)
            .map(Duration::from_nanos)
    }

    pub(crate) fn write_time_of<Q>(&self, key: &Q) -> Option<u64>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        lock(&self.inner.shards[shard_index]).write_time_of(key, now)
    }

    /// Absolute expiration deadline in ticker nanoseconds.
    pub fn expires_at<Q>(&self, key: &Q) -> Option<u64>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        lock(&self.inner.shards[shard_index]).expires_at(key, now)
    }

    /// Remaining lifetime before expiry.
    pub fn remaining_duration<Q>(&self, key: &Q) -> Option<Duration>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.now();
        self.expires_at(key)
            .map(|at| Duration::from_nanos(at.saturating_sub(now)))
    }

    /// Absolute time at which an entry becomes refreshable.
    pub fn refreshable_at<Q>(&self, key: &Q) -> Option<u64>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let ttl = self.inner.config.refresh_ttl()?;
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        lock(&self.inner.shards[shard_index]).refreshable_at(key, now, ttl)
    }

    /// Keys with a refresh currently in flight.
    pub fn refreshing_keys(&self) -> Vec<K> {
        lock(&self.inner.shards[0]).refreshing_keys()
    }

    pub(crate) fn fixed_expiration(&self, access: bool) -> Option<u64> {
        let shard = lock(&self.inner.shards[0]);
        if access {
            shard.fixed_access()
        } else {
            shard.fixed_write()
        }
    }

    pub(crate) fn set_fixed_expiration(&self, access: bool, duration: u64) {
        let now = self.now();
        let mut removed = Vec::new();
        lock(&self.inner.shards[0]).set_fixed_duration(access, duration, now, &mut removed);
        self.notify(removed);
    }

    pub(crate) fn set_variable_expiry<Q>(&self, key: &Q, duration: u64)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        lock(&self.inner.shards[shard_index]).set_expiry(key, now, duration, &mut removed);
        self.notify(removed);
    }

    pub(crate) fn order_by_time(&self, access: bool, oldest: bool, limit: usize) -> Vec<(K, V)> {
        lock(&self.inner.shards[0]).order_by_time(access, oldest, limit)
    }

    pub(crate) fn order_by_expiry(&self, oldest: bool, limit: usize) -> Vec<(K, V)> {
        lock(&self.inner.shards[0]).order_by_expiry(oldest, limit)
    }

    /// Marks an entry for asynchronous refresh if a loader is configured.
    ///
    /// Returns `true` if a refresh task was scheduled. Concurrent requests
    /// for the same key are coalesced while a refresh is in flight.
    pub fn refresh<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if self.inner.config.loader.is_none() {
            return false;
        }
        let now = self.now();
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        let scheduled = {
            let mut shard = lock(&self.inner.shards[shard_index]);
            shard.start_refresh(key, now, &mut removed)
        };
        self.notify(removed);
        match scheduled {
            Some((key, write_time)) => {
                self.submit_refresh(key, write_time);
                true
            }
            None => false,
        }
    }

    /// Refreshes every supplied key, mirroring `LoadingCache.refreshAll`.
    pub fn refresh_all(&self, keys: impl IntoIterator<Item = K>) {
        for key in keys {
            self.refresh(&key);
        }
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    fn now(&self) -> u64 {
        self.inner.config.now()
    }

    fn hash_key<Q: Hash + ?Sized>(&self, key: &Q) -> u64 {
        self.inner.hasher.hash_one(key)
    }

    fn shard_index(&self, hash: u64) -> usize {
        (hash as usize) & self.inner.shard_mask
    }

    fn submit_refresh(&self, key: K, write_time: u64) {
        let Some(loader) = self.inner.config.loader.clone() else {
            return;
        };
        let cache = self.clone();
        let executor = Arc::clone(&self.inner.config.executor);
        executor(Box::new(move || cache.run_refresh(key, write_time, loader)));
    }

    fn run_refresh(&self, key: K, captured_write: u64, loader: Loader<K, V>) {
        let started = Instant::now();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loader(&key)));
        let value = match outcome {
            Ok(Some(value)) => value,
            Ok(None) | Err(_) => {
                self.record_load_failure(started);
                self.clear_refreshing(&key, captured_write);
                return;
            }
        };
        let weight = self.inner.config.weight(&key, &value);
        let now = self.now();
        let hash = self.hash_key(&key);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        {
            let mut shard = lock(&self.inner.shards[shard_index]);
            shard.finish_refresh(&key, captured_write, value, weight, now, &mut removed);
        }
        self.notify(removed);
        self.record_load_success(started);
    }

    fn clear_refreshing(&self, key: &K, captured_write: u64) {
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        lock(&self.inner.shards[shard_index]).clear_refreshing(key, captured_write);
    }

    fn notify(&self, removed: Vec<RemovedEntry<K, V>>) {
        for entry in removed {
            if entry.cause.was_evicted() {
                self.record_eviction(entry.weight);
                if let Some(listener) = &self.inner.config.eviction_listener {
                    listener(&entry.key, &entry.value, entry.cause);
                }
            }
            if let Some(listener) = &self.inner.config.removal_listener {
                listener(&entry.key, &entry.value, entry.cause);
            }
        }
    }

    fn record_hit(&self) {
        if self.inner.config.record_stats {
            self.inner.stats.hit.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_miss(&self) {
        if self.inner.config.record_stats {
            self.inner.stats.miss.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn note_load_success(&self, started: Instant) {
        self.record_load_success(started);
    }

    pub(crate) fn note_load_failure(&self, started: Instant) {
        self.record_load_failure(started);
    }

    fn record_load_success(&self, started: Instant) {
        if self.inner.config.record_stats {
            self.inner
                .stats
                .load_success
                .fetch_add(1, Ordering::Relaxed);
            let nanos = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            self.inner
                .stats
                .total_load_nanos
                .fetch_add(nanos, Ordering::Relaxed);
        }
    }

    fn record_load_failure(&self, _started: Instant) {
        if self.inner.config.record_stats {
            self.inner
                .stats
                .load_failure
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_eviction(&self, weight: u32) {
        if self.inner.config.record_stats {
            self.inner.stats.evictions.fetch_add(1, Ordering::Relaxed);
            self.inner
                .stats
                .evicted_weight
                .fetch_add(weight as u64, Ordering::Relaxed);
        }
    }
}

/// A snapshot of a cache entry, mirroring Caffeine's `Policy.CacheEntry`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry<K, V> {
    /// The entry's key.
    pub key: K,
    /// The entry's value.
    pub value: V,
    /// The entry's weight.
    pub weight: u32,
    /// Absolute ticker reading at which the entry expires.
    pub expires_at: u64,
    /// Absolute ticker reading at which the entry becomes refreshable.
    pub refreshable_at: u64,
    /// Ticker reading when the snapshot was taken.
    pub snapshot_at: u64,
}

impl<K, V> CacheEntry<K, V> {
    /// The entry's key.
    pub fn key(&self) -> &K {
        &self.key
    }

    /// The entry's value.
    pub fn value(&self) -> &V {
        &self.value
    }

    /// Duration until the entry expires.
    pub fn expires_after(&self) -> Duration {
        Duration::from_nanos(self.expires_at.saturating_sub(self.snapshot_at))
    }

    /// Duration until the entry becomes refreshable.
    pub fn refreshable_after(&self) -> Duration {
        Duration::from_nanos(self.refreshable_at.saturating_sub(self.snapshot_at))
    }
}

/// A read-only view of the cache's eviction and expiration configuration,
/// mirroring Caffeine's `Policy`.
pub struct Policy<'a, K, V> {
    cache: &'a Cache<K, V>,
}

impl<K, V> Policy<'_, K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Maximum number of entries, or maximum total weight when weighted.
    /// Unbounded caches report a very large sentinel value.
    pub fn maximum(&self) -> u64 {
        lock(&self.cache.inner.shards[0]).maximum()
    }

    /// Returns `true` when eviction is driven by weights rather than entries.
    pub fn is_weighted(&self) -> bool {
        self.cache.inner.config.is_weighted
    }

    /// Sum of all entry weights. Equals the entry count for unweighted caches.
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    /// Approximate number of entries.
    pub fn estimated_size(&self) -> usize {
        self.cache.len()
    }

    /// Returns `true` when statistics are being recorded.
    pub fn is_recording_stats(&self) -> bool {
        self.cache.is_recording_stats()
    }

    /// Keys with a refresh in flight.
    pub fn refreshes(&self) -> Vec<K> {
        self.cache.refreshing_keys()
    }

    /// Returns the eviction policy view when the cache is bounded.
    pub fn eviction(&self) -> Option<Eviction<'_, K, V>> {
        self.cache
            .inner
            .config
            .bounded
            .then_some(Eviction { cache: self.cache })
    }

    /// Returns the fixed time-to-idle policy when configured.
    pub fn expire_after_access(&self) -> Option<FixedExpiration<'_, K, V>> {
        self.cache.fixed_expiration(true).map(|_| FixedExpiration {
            cache: self.cache,
            access: true,
        })
    }

    /// Returns the fixed time-to-live policy when configured.
    pub fn expire_after_write(&self) -> Option<FixedExpiration<'_, K, V>> {
        self.cache.fixed_expiration(false).map(|_| FixedExpiration {
            cache: self.cache,
            access: false,
        })
    }

    /// Returns the per-entry expiration view when a custom `Expiry` is used.
    pub fn expire_variably(&self) -> Option<VarExpiration<'_, K, V>> {
        self.is_variable_expiration()
            .then_some(VarExpiration { cache: self.cache })
    }

    /// Returns the refresh-after-write policy when configured.
    pub fn refresh_after_write(&self) -> Option<FixedRefresh<'_, K, V>> {
        self.cache
            .inner
            .config
            .refresh_ttl()
            .map(|_| FixedRefresh { cache: self.cache })
    }

    /// Returns the value without recording a hit or miss.
    pub fn get_if_present_quietly<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.peek(key)
    }

    /// Returns an entry snapshot without recording a hit or miss, mirroring
    /// `Policy.getEntryIfPresentQuietly`.
    pub fn get_entry_if_present_quietly(&self, key: K) -> Option<CacheEntry<K, V>> {
        let value = self.cache.peek(&key)?;
        let snapshot_at = self.cache.now();
        let weight = self.cache.weight_of(&key).unwrap_or(1);
        let expires_at = self.cache.expires_at(&key).unwrap_or(u64::MAX);
        let refreshable_at = self.cache.refreshable_at(&key).unwrap_or(u64::MAX);
        Some(CacheEntry {
            key,
            value,
            weight,
            expires_at,
            refreshable_at,
            snapshot_at,
        })
    }

    /// Clones every live key.
    pub fn keys(&self) -> Vec<K> {
        self.cache.keys()
    }

    /// Returns `true` when a custom per-entry [`Expiry`](crate::Expiry) is in
    /// effect.
    pub fn is_variable_expiration(&self) -> bool {
        self.cache.inner.config.expiration.is_custom()
    }

    /// Current admission window size.
    pub fn window_maximum(&self) -> u64 {
        self.cache
            .inner
            .shards
            .iter()
            .map(|shard| lock(shard).window_maximum())
            .sum()
    }
}

/// The size- or weight-bounded eviction policy view.
pub struct Eviction<'a, K, V> {
    cache: &'a Cache<K, V>,
}

impl<K, V> Eviction<'_, K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Returns `true` when the bound is measured in weight.
    pub fn is_weighted(&self) -> bool {
        self.cache.inner.config.is_weighted
    }

    /// Weight of a live entry, when the cache is weighted.
    pub fn weight_of<Q>(&self, key: &Q) -> Option<u32>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if !self.is_weighted() {
            return None;
        }
        self.cache.weight_of(key)
    }

    /// Accumulated weight, when the cache is weighted.
    pub fn weighted_size(&self) -> Option<u64> {
        self.is_weighted().then(|| self.cache.weighted_size())
    }

    /// Current maximum size or weight.
    pub fn maximum(&self) -> u64 {
        lock(&self.cache.inner.shards[0]).maximum()
    }

    /// Changes the maximum and eagerly evicts entries above it.
    pub fn set_maximum(&self, maximum: u64) {
        self.cache.set_maximum(maximum);
    }

    /// Snapshot from least to most likely to be retained.
    pub fn coldest(&self, limit: usize) -> Vec<(K, V)> {
        self.cache.coldest(limit)
    }

    /// Snapshot from most to least likely to be retained.
    pub fn hottest(&self, limit: usize) -> Vec<(K, V)> {
        self.cache.hottest(limit)
    }

    /// Snapshot from least to most likely to be retained, bounded by total
    /// weight rather than entry count.
    pub fn coldest_weighted(&self, weight_limit: u64) -> Vec<(K, V)> {
        self.weighted_snapshot(false, weight_limit)
    }

    /// Snapshot from most to least likely to be retained, bounded by total
    /// weight rather than entry count.
    pub fn hottest_weighted(&self, weight_limit: u64) -> Vec<(K, V)> {
        self.weighted_snapshot(true, weight_limit)
    }

    fn weighted_snapshot(&self, hottest: bool, weight_limit: u64) -> Vec<(K, V)> {
        let entries = if hottest {
            self.cache.hottest(usize::MAX)
        } else {
            self.cache.coldest(usize::MAX)
        };
        let mut total = 0u64;
        let mut selected = Vec::new();
        for (key, value) in entries {
            let weight = if self.is_weighted() {
                self.cache.weight_of(&key).unwrap_or(1) as u64
            } else {
                1
            };
            if total.saturating_add(weight) > weight_limit {
                break;
            }
            total += weight;
            selected.push((key, value));
        }
        selected
    }
}

/// A fixed time-to-idle or time-to-live expiration policy view.
pub struct FixedExpiration<'a, K, V> {
    cache: &'a Cache<K, V>,
    access: bool,
}

impl<K, V> FixedExpiration<'_, K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Configured duration.
    pub fn expires_after(&self) -> Duration {
        self.cache
            .fixed_expiration(self.access)
            .map(Duration::from_nanos)
            .unwrap_or_default()
    }

    /// Changes the duration for every entry and recomputes deadlines.
    pub fn set_expires_after(&self, duration: Duration) {
        self.cache
            .set_fixed_expiration(self.access, crate::ticker::nanos(duration));
    }

    /// Time since the entry's last access (idle) or write (TTL).
    pub fn age_of<Q>(&self, key: &Q) -> Option<Duration>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.age_of(key, self.access)
    }

    /// Oldest live entries under this policy.
    pub fn oldest(&self, limit: usize) -> Vec<(K, V)> {
        self.cache.order_by_time(self.access, true, limit)
    }

    /// Youngest live entries under this policy.
    pub fn youngest(&self, limit: usize) -> Vec<(K, V)> {
        self.cache.order_by_time(self.access, false, limit)
    }
}

/// The per-entry expiration policy view for a custom `Expiry`.
pub struct VarExpiration<'a, K, V> {
    cache: &'a Cache<K, V>,
}

impl<K, V> VarExpiration<'_, K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Remaining lifetime of an entry.
    pub fn get_expires_after<Q>(&self, key: &Q) -> Option<Duration>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.remaining_duration(key)
    }

    /// Resets an entry's lifetime to `duration` from now.
    pub fn set_expires_after<Q>(&self, key: &Q, duration: Duration)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let duration = crate::ticker::nanos(duration);
        self.cache.set_variable_expiry(key, duration);
    }

    /// Inserts or replaces an entry with an explicit lifetime, returning the
    /// previous value.
    pub fn put(&self, key: K, value: V, duration: Duration) -> Option<V> {
        let previous = self.cache.peek(&key);
        self.cache.insert(key.clone(), value);
        self.set_expires_after(&key, duration);
        previous
    }

    /// Inserts only when absent, returning the existing value otherwise.
    pub fn put_if_absent(&self, key: K, value: V, duration: Duration) -> Option<V> {
        if self.cache.put_if_absent(key.clone(), value).is_some() {
            return self.cache.peek(&key);
        }
        self.set_expires_after(&key, duration);
        None
    }

    /// Entries with the earliest deadlines.
    pub fn oldest(&self, limit: usize) -> Vec<(K, V)> {
        self.cache.order_by_expiry(true, limit)
    }

    /// Entries with the latest deadlines.
    pub fn youngest(&self, limit: usize) -> Vec<(K, V)> {
        self.cache.order_by_expiry(false, limit)
    }
}

/// The refresh-after-write policy view.
pub struct FixedRefresh<'a, K, V> {
    cache: &'a Cache<K, V>,
}

impl<K, V> FixedRefresh<'_, K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Configured refresh interval.
    pub fn refreshes_after(&self) -> Duration {
        self.cache
            .inner
            .config
            .refresh_ttl()
            .map(Duration::from_nanos)
            .unwrap_or_default()
    }

    /// Changes the refresh interval for future staleness checks.
    pub fn set_refreshes_after(&self, duration: Duration) {
        self.cache
            .inner
            .config
            .set_refresh_ttl(Some(crate::ticker::nanos(duration)));
    }

    /// Time since the entry was last written.
    pub fn age_of<Q>(&self, key: &Q) -> Option<Duration>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.age_of(key, false)
    }

    /// Entries written longest ago.
    pub fn oldest(&self, limit: usize) -> Vec<(K, V)> {
        self.cache.order_by_time(false, true, limit)
    }

    /// Entries written most recently.
    pub fn youngest(&self, limit: usize) -> Vec<(K, V)> {
        self.cache.order_by_time(false, false, limit)
    }
}

/// A live, map-shaped view of a [`Cache`], returned by [`Cache::as_map`].
///
/// Mutations and reads are forwarded to the cache. Iteration returns a
/// snapshot of the entries.
pub struct CacheMap<'a, K, V> {
    cache: &'a Cache<K, V>,
}

impl<K, V> CacheMap<'_, K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Returns a clone of the cached value.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.get(key)
    }

    /// Returns `true` when a live entry exists.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.contains_key(key)
    }

    /// Inserts or replaces an entry, returning the previous value.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.cache.put_returning(key, value)
    }

    /// Inserts only when absent, returning the existing value otherwise.
    pub fn put_if_absent(&self, key: K, value: V) -> Option<V> {
        self.cache.put_if_absent(key, value)
    }

    /// Removes an entry, returning its value if it was present.
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.invalidate(key)
    }

    /// Number of held entries.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns `true` when the cache holds no entries.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Removes every entry.
    pub fn clear(&self) {
        self.cache.invalidate_all();
    }

    /// Retains only entries matching the predicate.
    pub fn retain<F>(&self, predicate: F)
    where
        F: FnMut(&K, &V) -> bool,
    {
        self.cache.retain(predicate);
    }

    /// Snapshot of every entry.
    pub fn iter(&self) -> std::vec::IntoIter<(K, V)> {
        self.cache.iter()
    }

    /// Snapshot of every key.
    pub fn keys(&self) -> Vec<K> {
        self.cache.keys()
    }

    /// Snapshot of every value.
    pub fn values(&self) -> Vec<V> {
        self.cache.iter().map(|(_, value)| value).collect()
    }
}

fn shard_count(_capacity: u64) -> usize {
    // The eviction policy is global in Caffeine: one admission window, one
    // segmented main space, one sketch. Keeping a single shard preserves that
    // exactly; multiple shards would partition the policy and change which
    // entries are admitted and evicted.
    1
}

fn distribute_capacity(capacity: u64, shards: usize) -> Vec<u64> {
    let divisor = shards as u64;
    let base = capacity / divisor;
    let remainder = capacity % divisor;
    (0..divisor)
        .map(|index| base + u64::from(index < remainder))
        .collect()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Cache state is consistent across panics; recover rather than propagate
    // poisoning to innocent threads.
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
