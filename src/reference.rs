//! Weak and soft reference caches, mirroring Caffeine's `weakKeys`,
//! `weakValues`, and `softValues` builder options.
//!
//! Rust has no tracing garbage collector, so a cache cannot discover that a
//! weakly held value died. Instead the caller supplies `Arc` handles on insert
//! and the cache stores `Weak` references internally; a live `Arc` keeps the
//! entry reachable, and dropping it makes the entry collectable. Dead entries
//! are removed on access, during [`ReferenceCache::clean_up`], or by the next
//! size-maintenance pass.
//!
//! Soft references have no Rust equivalent. `soft_values` stores the same
//! `Weak` handle as `weak_values` and exposes [`ReferenceCache::clean_up`] as
//! the manual memory-pressure hook; the distinction is therefore a policy
//! choice made by the caller, not an automatic collector decision.

use std::hash::{Hash, Hasher};
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::builder::CacheBuilder;
use crate::cache::Cache;
use crate::expiry::Expiry;
use crate::removal::{EvictionListener, RemovalCause, RemovalListener};
use crate::stats::CacheStats;
use crate::ticker::{SharedTicker, Ticker};

/// How strongly a key or value is held by a [`ReferenceCache`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReferenceStrength {
    /// A strong `Arc` reference. Keys use value equality.
    Strong,
    /// A `Weak` reference. Weak keys use pointer identity, like Caffeine's
    /// `weakKeys`.
    Weak,
    /// A manually purged weak reference. Rust has no soft references.
    Soft,
}

pub(crate) struct RefKey<K>(RefKind<K>);

impl<K> Clone for RefKey<K> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

enum RefKind<T> {
    Strong(Arc<T>),
    Weak(Weak<T>),
}

impl<T> Clone for RefKind<T> {
    fn clone(&self) -> Self {
        match self {
            RefKind::Strong(value) => RefKind::Strong(Arc::clone(value)),
            RefKind::Weak(value) => RefKind::Weak(Weak::clone(value)),
        }
    }
}

impl<K> RefKey<K> {
    fn strong(key: Arc<K>) -> Self {
        Self(RefKind::Strong(key))
    }

    fn weak(key: Arc<K>) -> Self {
        Self(RefKind::Weak(Arc::downgrade(&key)))
    }

    pub(crate) fn upgrade(&self) -> Option<Arc<K>> {
        match &self.0 {
            RefKind::Strong(key) => Some(Arc::clone(key)),
            RefKind::Weak(key) => key.upgrade(),
        }
    }
}

impl<K: Hash> Hash for RefKey<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            RefKind::Strong(key) => key.hash(state),
            RefKind::Weak(key) => (key.as_ptr() as usize).hash(state),
        }
    }
}

impl<K: Eq> PartialEq for RefKey<K> {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (RefKind::Strong(a), RefKind::Strong(b)) => a == b,
            (RefKind::Weak(a), RefKind::Weak(b)) => Weak::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl<K: Eq> Eq for RefKey<K> {}

pub(crate) struct RefValue<V>(RefKind<V>);

impl<V> Clone for RefValue<V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<V> RefValue<V> {
    fn strong(value: Arc<V>) -> Self {
        Self(RefKind::Strong(value))
    }

    fn weak(value: Arc<V>) -> Self {
        Self(RefKind::Weak(Arc::downgrade(&value)))
    }

    pub(crate) fn upgrade(&self) -> Option<Arc<V>> {
        match &self.0 {
            RefKind::Strong(value) => Some(Arc::clone(value)),
            RefKind::Weak(value) => value.upgrade(),
        }
    }
}

/// A cache whose keys and values may be held weakly.
///
/// Insert and access entries with `Arc` handles:
///
/// ```
/// use caffeine::ReferenceCache;
/// use std::sync::Arc;
///
/// let cache: ReferenceCache<u64, String> = ReferenceCache::builder()
///     .maximum_size(100)
///     .weak_values()
///     .build();
///
/// let value = Arc::new("hello".to_string());
/// cache.insert(Arc::new(7), Arc::clone(&value));
/// assert_eq!(cache.get(&Arc::new(7)).as_deref(), Some(&"hello".to_string()));
///
/// drop(value);
/// cache.clean_up();
/// assert!(cache.get(&Arc::new(7)).is_none());
/// ```
pub struct ReferenceCache<K, V> {
    inner: Cache<RefKey<K>, RefValue<V>>,
    weak_keys: bool,
    weak_values: bool,
}

impl<K, V> Clone for ReferenceCache<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            weak_keys: self.weak_keys,
            weak_values: self.weak_values,
        }
    }
}

impl<K, V> ReferenceCache<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Returns a new [`ReferenceCacheBuilder`].
    pub fn builder() -> ReferenceCacheBuilder<K, V> {
        ReferenceCacheBuilder::new()
    }

    fn key_ref(&self, key: &Arc<K>) -> RefKey<K> {
        if self.weak_keys {
            RefKey::weak(Arc::clone(key))
        } else {
            RefKey::strong(Arc::clone(key))
        }
    }

    /// Inserts or replaces an entry. The cache stores a weak handle when the
    /// corresponding strength is weak or soft.
    pub fn insert(&self, key: Arc<K>, value: Arc<V>) {
        let key_ref = self.key_ref(&key);
        let value_ref = if self.weak_values {
            RefValue::weak(value)
        } else {
            RefValue::strong(value)
        };
        self.inner.insert(key_ref, value_ref);
    }

    /// Returns a strong handle to the value when both sides are still alive.
    pub fn get(&self, key: &Arc<K>) -> Option<Arc<V>> {
        let key_ref = self.key_ref(key);
        let value = self.inner.peek_quietly(&key_ref)?;
        match value.upgrade() {
            Some(value) => {
                // Record the access and move the entry in the eviction order.
                let _ = self.inner.get(&key_ref);
                Some(value)
            }
            None => {
                self.inner
                    .remove_with_cause(&key_ref, RemovalCause::Collected);
                // Account for the lookup as a miss after collecting the entry.
                let _ = self.inner.get(&key_ref);
                None
            }
        }
    }

    /// Returns the value without recording a hit or miss.
    pub fn peek(&self, key: &Arc<K>) -> Option<Arc<V>> {
        let key_ref = self.key_ref(key);
        self.inner.peek_quietly(&key_ref)?.upgrade()
    }

    /// Returns `true` when a live entry exists.
    pub fn contains_key(&self, key: &Arc<K>) -> bool {
        self.peek(key).is_some()
    }

    /// Removes an entry.
    pub fn invalidate(&self, key: &Arc<K>) -> Option<Arc<V>> {
        let key_ref = self.key_ref(key);
        self.inner
            .remove_with_cause(&key_ref, RemovalCause::Explicit)
            .and_then(|value| value.upgrade())
    }

    /// Removes every entry.
    pub fn invalidate_all(&self) {
        self.inner.invalidate_all();
    }

    /// Removes every entry.
    pub fn clear(&self) {
        self.invalidate_all();
    }

    /// Purges entries whose weak key or value has been dropped.
    pub fn clean_up(&self) {
        self.inner.clean_up();
        let dead: Vec<RefKey<K>> = self
            .inner
            .iter()
            .filter_map(|(key, value)| {
                if key.upgrade().is_none() || value.upgrade().is_none() {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();
        for key in dead {
            self.inner.remove_with_cause(&key, RemovalCause::Collected);
        }
    }

    /// Number of live entries after purging collected references.
    pub fn len(&self) -> usize {
        self.clean_up();
        self.inner.len()
    }

    /// Returns `true` when the cache holds no live entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate number of live entries.
    pub fn estimated_size(&self) -> usize {
        self.len()
    }

    /// Sum of entry weights.
    pub fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }

    /// Clones every live entry.
    pub fn iter(&self) -> Vec<(Arc<K>, Arc<V>)> {
        self.inner
            .iter()
            .filter_map(|(key, value)| Some((key.upgrade()?, value.upgrade()?)))
            .collect()
    }

    /// Statistics snapshot, including collected entries as evictions.
    pub fn stats(&self) -> CacheStats {
        self.inner.stats()
    }
}

type ReferenceWeigher<K, V> = Arc<dyn Fn(&K, &V) -> u32 + Send + Sync + 'static>;

/// Builds a [`ReferenceCache`].
pub struct ReferenceCacheBuilder<K, V> {
    key_strength: ReferenceStrength,
    value_strength: ReferenceStrength,
    maximum_size: Option<usize>,
    maximum_weight: Option<u64>,
    initial_capacity: usize,
    weigher: Option<ReferenceWeigher<K, V>>,
    expire_after_write: Option<Duration>,
    expire_after_access: Option<Duration>,
    expiry: Option<Arc<dyn Expiry<K, V>>>,
    record_stats: bool,
    ticker: Option<SharedTicker>,
    removal_listener: Option<RemovalListener<K, V>>,
    eviction_listener: Option<EvictionListener<K, V>>,
}

impl<K, V> ReferenceCacheBuilder<K, V> {
    /// Creates a builder with strong keys and values.
    pub fn new() -> Self {
        Self {
            key_strength: ReferenceStrength::Strong,
            value_strength: ReferenceStrength::Strong,
            maximum_size: None,
            maximum_weight: None,
            initial_capacity: 0,
            weigher: None,
            expire_after_write: None,
            expire_after_access: None,
            expiry: None,
            record_stats: false,
            ticker: None,
            removal_listener: None,
            eviction_listener: None,
        }
    }

    /// Holds keys weakly and compares them by pointer identity.
    pub fn weak_keys(mut self) -> Self {
        self.key_strength = ReferenceStrength::Weak;
        self
    }

    /// Holds values weakly.
    pub fn weak_values(mut self) -> Self {
        self.value_strength = ReferenceStrength::Weak;
        self
    }

    /// Holds values through a manually purged weak handle. Rust has no soft
    /// references; see the module documentation.
    pub fn soft_values(mut self) -> Self {
        self.value_strength = ReferenceStrength::Soft;
        self
    }

    /// Evicts entries so the cache holds at most `maximum` entries.
    pub fn maximum_size(mut self, maximum: usize) -> Self {
        self.maximum_size = Some(maximum);
        self
    }

    /// Bounds the total weight. Requires [`weigher`](Self::weigher).
    pub fn maximum_weight(mut self, maximum: u64) -> Self {
        self.maximum_weight = Some(maximum);
        self
    }

    /// Approximate initial table size.
    pub fn initial_capacity(mut self, initial_capacity: usize) -> Self {
        self.initial_capacity = initial_capacity;
        self
    }

    /// Expires entries after their last write.
    pub fn expire_after_write(mut self, duration: Duration) -> Self {
        self.expire_after_write = Some(duration);
        self
    }

    /// Expires entries after their last access.
    pub fn expire_after_access(mut self, duration: Duration) -> Self {
        self.expire_after_access = Some(duration);
        self
    }

    /// Uses a custom per-entry expiration policy.
    pub fn expire_after<E>(mut self, expiry: E) -> Self
    where
        E: Expiry<K, V>,
    {
        self.expiry = Some(Arc::new(expiry));
        self
    }

    /// Computes an entry's weight from the live key and value.
    pub fn weigher<F>(mut self, weigher: F) -> Self
    where
        F: Fn(&K, &V) -> u32 + Send + Sync + 'static,
    {
        self.weigher = Some(Arc::new(weigher));
        self
    }

    /// Enables statistics collection.
    pub fn record_stats(mut self, enabled: bool) -> Self {
        self.record_stats = enabled;
        self
    }

    /// Uses a custom time source.
    pub fn ticker<T: Ticker>(mut self, ticker: T) -> Self {
        self.ticker = Some(Arc::new(ticker));
        self
    }

    /// Called for every removed entry whose key and value are still alive.
    pub fn removal_listener<F>(mut self, listener: F) -> Self
    where
        F: Fn(&K, &V, RemovalCause) + Send + Sync + 'static,
    {
        self.removal_listener = Some(Arc::new(listener));
        self
    }

    /// Called for evicted entries whose key and value are still alive.
    pub fn eviction_listener<F>(mut self, listener: F) -> Self
    where
        F: Fn(&K, &V, RemovalCause) + Send + Sync + 'static,
    {
        self.eviction_listener = Some(Arc::new(listener));
        self
    }
}

impl<K, V> Default for ReferenceCacheBuilder<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ReferenceCacheBuilder<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Builds the reference cache.
    pub fn build(self) -> ReferenceCache<K, V> {
        let weak_keys = self.key_strength != ReferenceStrength::Strong;
        let weak_values = self.value_strength != ReferenceStrength::Strong;

        let mut inner: CacheBuilder<RefKey<K>, RefValue<V>> =
            Cache::builder().initial_capacity(self.initial_capacity);
        inner = match (self.maximum_size, self.maximum_weight) {
            (Some(size), None) => inner.maximum_size(size),
            (None, Some(weight)) => inner.maximum_weight(weight),
            (None, None) => inner,
            (Some(_), Some(_)) => {
                panic!("maximum_size and maximum_weight are mutually exclusive")
            }
        };
        if let Some(weigher) = self.weigher {
            inner = inner.weigher(move |key: &RefKey<K>, value: &RefValue<V>| {
                match (key.upgrade(), value.upgrade()) {
                    (Some(key), Some(value)) => weigher(&key, &value),
                    _ => 0,
                }
            });
        }
        if let Some(duration) = self.expire_after_write {
            inner = inner.expire_after_write(duration);
        }
        if let Some(duration) = self.expire_after_access {
            inner = inner.expire_after_access(duration);
        }
        if let Some(expiry) = self.expiry {
            inner = inner.expire_after(ReferenceExpiry { inner: expiry });
        }
        if let Some(ticker) = self.ticker {
            inner = inner.ticker(ticker);
        }
        if let Some(listener) = self.removal_listener {
            inner = inner.removal_listener(move |key: &RefKey<K>, value: &RefValue<V>, cause| {
                if let (Some(key), Some(value)) = (key.upgrade(), value.upgrade()) {
                    listener(&key, &value, cause);
                }
            });
        }
        if let Some(listener) = self.eviction_listener {
            inner = inner.eviction_listener(move |key: &RefKey<K>, value: &RefValue<V>, cause| {
                if let (Some(key), Some(value)) = (key.upgrade(), value.upgrade()) {
                    listener(&key, &value, cause);
                }
            });
        }
        let inner = inner.record_stats(self.record_stats).build();
        ReferenceCache {
            inner,
            weak_keys,
            weak_values,
        }
    }
}

struct ReferenceExpiry<K, V> {
    inner: Arc<dyn Expiry<K, V>>,
}

impl<K, V> Expiry<RefKey<K>, RefValue<V>> for ReferenceExpiry<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn expire_after_create(
        &self,
        key: &RefKey<K>,
        value: &RefValue<V>,
        now: u64,
    ) -> Option<Duration> {
        match (key.upgrade(), value.upgrade()) {
            (Some(key), Some(value)) => self.inner.expire_after_create(&key, &value, now),
            _ => None,
        }
    }

    fn expire_after_update(
        &self,
        key: &RefKey<K>,
        value: &RefValue<V>,
        now: u64,
        current: Option<Duration>,
    ) -> Option<Duration> {
        match (key.upgrade(), value.upgrade()) {
            (Some(key), Some(value)) => self.inner.expire_after_update(&key, &value, now, current),
            _ => None,
        }
    }

    fn expire_after_read(
        &self,
        key: &RefKey<K>,
        value: &RefValue<V>,
        now: u64,
        current: Option<Duration>,
    ) -> Option<Duration> {
        match (key.upgrade(), value.upgrade()) {
            (Some(key), Some(value)) => self.inner.expire_after_read(&key, &value, now, current),
            _ => None,
        }
    }
}
