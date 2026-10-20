//! The sharded, thread-safe cache and its policy view.

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
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
        let expire_after_write = config.expire_after_write;
        let expire_after_access = config.expire_after_access;
        let shards: Vec<Mutex<Shard<K, V>>> = capacities
            .into_iter()
            .map(|capacity| {
                Mutex::new(Shard::new(
                    capacity,
                    max_capacity,
                    initial_per_shard,
                    expire_after_write,
                    expire_after_access,
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
    /// Concurrent misses for the same key may each run `init`; the last
    /// insertion wins. Use a single-flight helper if that is not acceptable.
    pub fn get_with<F>(&self, key: K, init: F) -> V
    where
        F: FnOnce(&K) -> V,
    {
        if let Some(value) = self.get(&key) {
            return value;
        }
        let started = Instant::now();
        let value = init(&key);
        self.record_load_success(started);
        self.insert(key, value.clone());
        value
    }

    /// Like [`get_with`](Self::get_with), but the loader may fail.
    pub fn try_get_with<E, F>(&self, key: K, init: F) -> Result<V, E>
    where
        F: FnOnce(&K) -> Result<V, E>,
    {
        if let Some(value) = self.get(&key) {
            return Ok(value);
        }
        let started = Instant::now();
        match init(&key) {
            Ok(value) => {
                self.record_load_success(started);
                self.insert(key, value.clone());
                Ok(value)
            }
            Err(error) => {
                self.record_load_failure(started);
                Err(error)
            }
        }
    }

    /// Inserts or replaces an entry, notifying listeners for a replaced value.
    pub fn insert(&self, key: K, value: V) {
        let now = self.now();
        let hash = self.hash_key(&key);
        let weight = self.inner.config.weight(&key, &value);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        {
            let mut shard = lock(&self.inner.shards[shard_index]);
            shard.insert(key, value, hash, weight, now, &mut removed);
        }
        self.notify(removed);
    }

    /// Alias for [`insert`](Self::insert), matching `ConcurrentMap::put`.
    pub fn put(&self, key: K, value: V) {
        self.insert(key, value);
    }

    /// Removes an entry, returning its value if it was present.
    pub fn invalidate<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_key(key);
        let shard_index = self.shard_index(hash);
        let mut removed = Vec::new();
        let value = {
            let mut shard = lock(&self.inner.shards[shard_index]);
            shard.remove(key, RemovalCause::Explicit, &mut removed)
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
    /// Expired entries are skipped but not removed; call
    /// [`clean_up`](Self::clean_up) for that.
    pub fn iter(&self) -> std::vec::IntoIter<(K, V)> {
        let now = self.now();
        let mut entries = Vec::new();
        for shard in self.inner.shards.iter() {
            entries.extend(lock(shard).snapshot(now));
        }
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

/// A read-only view of the cache's eviction and expiration configuration.
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
        self.cache.inner.config.max_capacity
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

    /// Configured time-to-idle.
    pub fn expire_after_access(&self) -> Option<Duration> {
        self.cache
            .inner
            .config
            .expire_after_access
            .map(Duration::from_nanos)
    }

    /// Configured time-to-live.
    pub fn expire_after_write(&self) -> Option<Duration> {
        self.cache
            .inner
            .config
            .expire_after_write
            .map(Duration::from_nanos)
    }

    /// Configured refresh interval.
    pub fn refresh_after_write(&self) -> Option<Duration> {
        self.cache
            .inner
            .config
            .refresh_after_write
            .map(Duration::from_nanos)
    }
}

fn shard_count(capacity: u64) -> usize {
    let parallelism = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    let max_shards = parallelism.saturating_mul(4).clamp(1, 64);
    let mut count = 1usize;
    while count * 2 <= max_shards {
        count *= 2;
    }
    // Small caches would be fragmented by too many shards.
    while count > 1 && capacity / (count as u64) < 16 {
        count /= 2;
    }
    count
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
