//! Asynchronous cache views, mirroring Caffeine's `AsyncCache` and
//! `AsyncLoadingCache`.
//!
//! Rust has no `CompletableFuture`; these views expose `async` methods instead.
//! A miss computes the value on the caller's executor, and concurrent misses
//! for the same key are coalesced: one caller runs the initializer while the
//! others await its result. If the leader is cancelled, fails, or panics, the
//! waiters wake and retry, so a failed load is never shared.
//!
//! The synchronous cache remains available through
//! [`AsyncCache::synchronous`].

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use crate::cache::{Cache, CacheMap, Policy};
use crate::stats::CacheStats;

/// A future returned by [`AsyncLoadingCache::get`].
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// An asynchronous view over a [`Cache`].
pub struct AsyncCache<K, V> {
    cache: Cache<K, V>,
    loads: Arc<Mutex<HashMap<K, Arc<LoadSlot<V>>>>>,
}

impl<K, V> Clone for AsyncCache<K, V> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            loads: Arc::clone(&self.loads),
        }
    }
}

impl<K, V> AsyncCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Wraps a synchronous cache.
    pub fn new(cache: Cache<K, V>) -> Self {
        Self {
            cache,
            loads: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the underlying synchronous cache.
    pub fn synchronous(&self) -> &Cache<K, V> {
        &self.cache
    }

    /// Returns a policy view for the underlying cache.
    pub fn policy(&self) -> Policy<'_, K, V> {
        self.cache.policy()
    }

    /// Returns a clone of the cached value without loading.
    pub async fn get(&self, key: K) -> Option<V> {
        self.cache.get(&key)
    }

    /// Returns a live map view over the synchronous cache.
    pub fn as_map(&self) -> CacheMap<'_, K, V> {
        self.cache.as_map()
    }

    /// Inserts every entry from `entries`.
    pub fn put_all(&self, entries: impl IntoIterator<Item = (K, V)>) {
        for (key, value) in entries {
            self.cache.insert(key, value);
        }
    }

    /// Returns the entries for `keys`, invoking `mapping` once for the
    /// missing keys. Every returned entry is stored, but only requested keys
    /// appear in the result.
    pub async fn get_all<F>(&self, keys: impl IntoIterator<Item = K>, mapping: F) -> HashMap<K, V>
    where
        F: FnOnce(&[K]) -> HashMap<K, V>,
    {
        let mut result = HashMap::new();
        let mut missing = Vec::new();
        for key in keys {
            match self.cache.get(&key) {
                Some(value) => {
                    result.insert(key, value);
                }
                None => missing.push(key),
            }
        }
        if missing.is_empty() {
            return result;
        }
        for (key, value) in mapping(&missing) {
            let requested = missing.contains(&key);
            self.cache.insert(key.clone(), value.clone());
            if requested {
                result.insert(key, value);
            }
        }
        result
    }

    /// Like [`get_all`](Self::get_all), but `mapping` is asynchronous.
    pub async fn get_all_async<F, Fut>(
        &self,
        keys: impl IntoIterator<Item = K>,
        mapping: F,
    ) -> HashMap<K, V>
    where
        F: FnOnce(&[K]) -> Fut,
        Fut: Future<Output = HashMap<K, V>>,
    {
        let mut result = HashMap::new();
        let mut missing = Vec::new();
        for key in keys {
            match self.cache.get(&key) {
                Some(value) => {
                    result.insert(key, value);
                }
                None => missing.push(key),
            }
        }
        if missing.is_empty() {
            return result;
        }
        for (key, value) in mapping(&missing).await {
            let requested = missing.contains(&key);
            self.cache.insert(key.clone(), value.clone());
            if requested {
                result.insert(key, value);
            }
        }
        result
    }

    /// Returns the value, computing it with `init` on a miss.
    pub async fn get_with<F>(&self, key: K, init: F) -> V
    where
        F: FnOnce(&K) -> V + Send + 'static,
    {
        let started = Instant::now();
        let mut init = Some(init);
        loop {
            if let Some(value) = self.cache.get(&key) {
                return value;
            }
            match self.begin(&key) {
                Begin::Follower(response) => {
                    if let Some(value) = response.await {
                        return value;
                    }
                }
                Begin::Leader(slot) => {
                    let guard = LoadLeader::new(Arc::clone(&self.loads), key.clone(), slot.clone());
                    let init = init.take().expect("initializer is available to the leader");
                    let value = init(&key);
                    self.cache.insert(key.clone(), value.clone());
                    slot.complete(value.clone());
                    drop(guard);
                    self.cache.note_load_success(started);
                    return value;
                }
            }
        }
    }

    /// Returns the value, computing it with an asynchronous `init` on a miss.
    pub async fn get_with_async<F, Fut>(&self, key: K, init: F) -> V
    where
        F: FnOnce(K) -> Fut + Send + 'static,
        Fut: Future<Output = V> + Send + 'static,
    {
        let started = Instant::now();
        let mut init = Some(init);
        loop {
            if let Some(value) = self.cache.get(&key) {
                return value;
            }
            match self.begin(&key) {
                Begin::Follower(response) => {
                    if let Some(value) = response.await {
                        return value;
                    }
                }
                Begin::Leader(slot) => {
                    let guard = LoadLeader::new(Arc::clone(&self.loads), key.clone(), slot.clone());
                    let init = init.take().expect("initializer is available to the leader");
                    let value = init(key.clone()).await;
                    self.cache.insert(key.clone(), value.clone());
                    slot.complete(value.clone());
                    drop(guard);
                    self.cache.note_load_success(started);
                    return value;
                }
            }
        }
    }

    /// Returns the value or the error produced by an asynchronous `init`.
    ///
    /// Waiters retry after a failure; the error is only returned to the
    /// caller that ran the initializer.
    pub async fn try_get_with<F, Fut, E>(&self, key: K, init: F) -> Result<V, E>
    where
        F: FnOnce(K) -> Fut + Send + 'static,
        Fut: Future<Output = Result<V, E>> + Send + 'static,
        E: Send + 'static,
    {
        let started = Instant::now();
        let mut init = Some(init);
        loop {
            if let Some(value) = self.cache.get(&key) {
                return Ok(value);
            }
            match self.begin(&key) {
                Begin::Follower(response) => {
                    if let Some(value) = response.await {
                        return Ok(value);
                    }
                }
                Begin::Leader(slot) => {
                    let guard = LoadLeader::new(Arc::clone(&self.loads), key.clone(), slot.clone());
                    let init = init.take().expect("initializer is available to the leader");
                    match init(key.clone()).await {
                        Ok(value) => {
                            self.cache.insert(key.clone(), value.clone());
                            slot.complete(value.clone());
                            drop(guard);
                            self.cache.note_load_success(started);
                            return Ok(value);
                        }
                        Err(error) => {
                            self.cache.note_load_failure(started);
                            drop(guard);
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    /// Inserts a value immediately.
    pub fn put(&self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    /// Inserts the value produced by a future.
    pub async fn put_future<F>(&self, key: K, value: F)
    where
        F: Future<Output = V>,
    {
        self.cache.insert(key, value.await);
    }

    /// Removes an entry.
    pub fn invalidate<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.invalidate(key)
    }

    /// Removes every entry.
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Performs expiration maintenance.
    pub fn clean_up(&self) {
        self.cache.clean_up();
    }

    /// Number of held entries.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns `true` when the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Approximate number of entries.
    pub fn estimated_size(&self) -> usize {
        self.cache.estimated_size()
    }

    /// Statistics snapshot.
    pub fn stats(&self) -> CacheStats {
        self.cache.stats()
    }

    fn begin(&self, key: &K) -> Begin<V> {
        let mut loads = lock(&self.loads);
        if let Some(slot) = loads.get(key) {
            if slot.owner == thread::current().id() {
                panic!("recursive load of the same key");
            }
            return Begin::Follower(LoadResponse {
                slot: Arc::clone(slot),
            });
        }
        let slot = Arc::new(LoadSlot {
            state: Mutex::new((LoadState::Pending, Vec::new())),
            owner: thread::current().id(),
        });
        loads.insert(key.clone(), Arc::clone(&slot));
        Begin::Leader(slot)
    }
}

/// An [`AsyncCache`] with a built-in asynchronous loader.
pub struct AsyncLoadingCache<K, V> {
    cache: AsyncCache<K, V>,
    loader: Arc<dyn Fn(K) -> BoxFuture<V> + Send + Sync + 'static>,
    refresh_after_write: Option<Duration>,
    refreshes: Arc<Mutex<HashSet<K>>>,
}

impl<K, V> Clone for AsyncLoadingCache<K, V> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            loader: Arc::clone(&self.loader),
            refresh_after_write: self.refresh_after_write,
            refreshes: Arc::clone(&self.refreshes),
        }
    }
}

impl<K, V> AsyncLoadingCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Creates a loading cache over `cache`.
    pub fn new<F>(cache: Cache<K, V>, loader: F) -> Self
    where
        F: Fn(K) -> BoxFuture<V> + Send + Sync + 'static,
    {
        Self {
            cache: AsyncCache::new(cache),
            loader: Arc::new(loader),
            refresh_after_write: None,
            refreshes: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Refreshes entries in the background after they have not been written
    /// for `duration`.
    pub fn with_refresh_after_write(mut self, duration: Duration) -> Self {
        self.refresh_after_write = Some(duration);
        self
    }

    /// Returns the cached value, loading it on a miss.
    pub async fn get(&self, key: K) -> V {
        let loader = Arc::clone(&self.loader);
        let value = self
            .cache
            .get_with_async(key.clone(), move |key| loader(key))
            .await;
        self.refresh_if_stale(&key);
        value
    }

    fn refresh_if_stale(&self, key: &K) {
        let Some(ttl) = self.refresh_after_write else {
            return;
        };
        let age = self.cache.synchronous().age_of(key, false);
        if age.map_or(true, |age| age < ttl) {
            return;
        }
        if !lock(&self.refreshes).insert(key.clone()) {
            return;
        }

        let loader = Arc::clone(&self.loader);
        let cache = self.cache.synchronous().clone();
        let pending = Arc::clone(&self.refreshes);
        let load_key = key.clone();
        let tracked_key = key.clone();
        let spawned = thread::Builder::new()
            .name("caffeine-async-refresh".to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    block_on(loader(load_key))
                }));
                if let Ok(value) = outcome {
                    if cache.contains_key(&tracked_key) {
                        cache.insert(tracked_key.clone(), value);
                    }
                }
                lock(&pending).remove(&tracked_key);
            });
        if spawned.is_err() {
            lock(&self.refreshes).remove(key);
        }
    }

    /// Returns every requested key, loading missing values one at a time.
    ///
    /// Caffeine loads missing keys concurrently with `CompletableFuture`;
    /// without an async runtime dependency this method awaits them in order,
    /// producing the same result.
    pub async fn get_all(&self, keys: impl IntoIterator<Item = K>) -> HashMap<K, V> {
        let mut result = HashMap::new();
        for key in keys {
            let value = self.get(key.clone()).await;
            result.insert(key, value);
        }
        result
    }

    /// Returns the cached value without loading.
    pub async fn get_if_present<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.synchronous().get(key)
    }

    /// Inserts a value immediately.
    pub fn put(&self, key: K, value: V) {
        self.cache.put(key, value);
    }

    /// Inserts every entry.
    pub fn put_all(&self, entries: impl IntoIterator<Item = (K, V)>) {
        self.cache.put_all(entries);
    }

    /// Inserts the value produced by a future.
    pub async fn put_future<F>(&self, key: K, value: F)
    where
        F: Future<Output = V>,
    {
        self.cache.put_future(key, value).await;
    }

    /// Removes an entry.
    pub fn invalidate<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache.synchronous().invalidate(key)
    }

    /// Removes every entry.
    pub fn invalidate_all(&self) {
        self.cache.synchronous().invalidate_all();
    }

    /// Performs expiration maintenance.
    pub fn clean_up(&self) {
        self.cache.synchronous().clean_up();
    }

    /// Approximate number of entries.
    pub fn estimated_size(&self) -> usize {
        self.cache.synchronous().estimated_size()
    }

    /// Statistics snapshot.
    pub fn stats(&self) -> CacheStats {
        self.cache.synchronous().stats()
    }

    /// Returns a policy view for the underlying cache.
    pub fn policy(&self) -> Policy<'_, K, V> {
        self.cache.synchronous().policy()
    }

    /// Returns a live map view over the synchronous cache.
    pub fn as_map(&self) -> CacheMap<'_, K, V> {
        self.cache.synchronous().as_map()
    }

    /// Returns the underlying asynchronous cache.
    pub fn cache(&self) -> &AsyncCache<K, V> {
        &self.cache
    }

    /// Returns the underlying synchronous cache.
    pub fn synchronous(&self) -> &Cache<K, V> {
        self.cache.synchronous()
    }
}

enum Begin<V> {
    Leader(Arc<LoadSlot<V>>),
    Follower(LoadResponse<V>),
}

enum LoadState<V> {
    Pending,
    Ready(V),
    Failed,
}

struct LoadSlot<V> {
    state: Mutex<(LoadState<V>, Vec<Waker>)>,
    owner: ThreadId,
}

impl<V: Clone> LoadSlot<V> {
    fn complete(&self, value: V) {
        let mut state = lock(&self.state);
        state.0 = LoadState::Ready(value);
        for waker in state.1.drain(..) {
            waker.wake();
        }
    }
}

struct LoadResponse<V> {
    slot: Arc<LoadSlot<V>>,
}

impl<V: Clone> Future for LoadResponse<V> {
    type Output = Option<V>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = lock(&self.slot.state);
        match &state.0 {
            LoadState::Pending => {
                state.1.push(context.waker().clone());
                Poll::Pending
            }
            LoadState::Ready(value) => Poll::Ready(Some(value.clone())),
            LoadState::Failed => Poll::Ready(None),
        }
    }
}

struct LoadLeader<K, V>
where
    K: Hash + Eq + Clone,
{
    loads: Arc<Mutex<HashMap<K, Arc<LoadSlot<V>>>>>,
    key: K,
    slot: Arc<LoadSlot<V>>,
}

impl<K, V> LoadLeader<K, V>
where
    K: Hash + Eq + Clone,
{
    fn new(
        loads: Arc<Mutex<HashMap<K, Arc<LoadSlot<V>>>>>,
        key: K,
        slot: Arc<LoadSlot<V>>,
    ) -> Self {
        Self { loads, key, slot }
    }
}

impl<K, V> Drop for LoadLeader<K, V>
where
    K: Hash + Eq + Clone,
{
    fn drop(&mut self) {
        {
            let mut loads = lock(&self.loads);
            if loads
                .get(&self.key)
                .map(|slot| Arc::ptr_eq(slot, &self.slot))
                .unwrap_or(false)
            {
                loads.remove(&self.key);
            }
        }
        let mut state = lock(&self.slot.state);
        if matches!(state.0, LoadState::Pending) {
            state.0 = LoadState::Failed;
            for waker in state.1.drain(..) {
                waker.wake();
            }
        }
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWaker(thread::Thread);

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
