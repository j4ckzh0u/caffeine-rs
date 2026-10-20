//! A Guava-shaped adapter, mirroring Caffeine's `guava` module.
//!
//! Guava is a Java library; there is no binary-compatible Rust counterpart.
//! This module provides the same builder and cache vocabulary in Rust so a
//! port can keep its call sites recognizable. The adapter is a thin wrapper
//! over [`Cache`] and adds no policy behavior of its own.
//!
//! ```
//! use caffeine::guava::GuavaCache;
//! use std::time::Duration;
//!
//! let cache: GuavaCache<u64, u64> = GuavaCache::builder()
//!     .maximum_size(100)
//!     .expire_after_write(Duration::from_secs(60))
//!     .build();
//! cache.put(1, 10);
//! assert_eq!(cache.get_if_present(&1), Some(10));
//! ```

use std::borrow::Borrow;
use std::hash::Hash;
use std::time::Duration;

use crate::builder::CacheBuilder;
use crate::cache::{Cache, Policy};
use crate::expiry::Expiry;
use crate::removal::RemovalCause;
use crate::stats::CacheStats;
use crate::ticker::Ticker;

/// A cache exposed through Guava-style method names.
pub struct GuavaCache<K, V> {
    inner: Cache<K, V>,
}

impl<K, V> Clone for GuavaCache<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V> GuavaCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Returns a Guava-shaped builder.
    pub fn builder() -> GuavaCacheBuilder<K, V> {
        GuavaCacheBuilder {
            inner: Cache::builder(),
        }
    }

    /// Returns the wrapped cache.
    pub fn into_inner(self) -> Cache<K, V> {
        self.inner
    }

    /// Guava's `getIfPresent`.
    pub fn get_if_present<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(key)
    }

    /// Guava's `get(key, Callable)`.
    pub fn get_with<F>(&self, key: K, loader: F) -> V
    where
        F: FnOnce(&K) -> V,
    {
        self.inner.get_with(key, loader)
    }

    /// Guava's `getAllPresent`.
    pub fn get_all_present<Q>(&self, keys: &[&Q]) -> Vec<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let snapshot: Vec<(K, V)> = self.inner.iter().collect();
        keys.iter()
            .filter_map(|key| {
                let key: &Q = key;
                snapshot
                    .iter()
                    .find(|(candidate, _)| candidate.borrow() == key)
                    .cloned()
            })
            .collect()
    }

    /// Guava's `put`.
    pub fn put(&self, key: K, value: V) {
        self.inner.insert(key, value);
    }

    /// Guava's `putAll`.
    pub fn put_all(&self, entries: impl IntoIterator<Item = (K, V)>) {
        for (key, value) in entries {
            self.inner.insert(key, value);
        }
    }

    /// Guava's `invalidate`.
    pub fn invalidate<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.invalidate(key);
    }

    /// Guava's `invalidateAll`.
    pub fn invalidate_all(&self) {
        self.inner.invalidate_all();
    }

    /// Guava's `cleanUp`.
    pub fn clean_up(&self) {
        self.inner.clean_up();
    }

    /// Guava's `size`.
    pub fn size(&self) -> usize {
        self.inner.len()
    }

    /// Guava's `stats`.
    pub fn stats(&self) -> CacheStats {
        self.inner.stats()
    }

    /// Snapshot of the cache as a map.
    pub fn as_map(&self) -> std::collections::HashMap<K, V> {
        self.inner.iter().collect()
    }

    /// Returns the wrapped cache's policy view.
    pub fn policy(&self) -> Policy<'_, K, V> {
        self.inner.policy()
    }
}

/// A Guava-shaped builder over [`CacheBuilder`].
pub struct GuavaCacheBuilder<K, V> {
    inner: CacheBuilder<K, V>,
}

impl<K, V> GuavaCacheBuilder<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Guava's `CacheBuilder.newBuilder`.
    pub fn new() -> Self {
        Self {
            inner: Cache::builder(),
        }
    }

    /// Guava's `maximumSize`.
    pub fn maximum_size(mut self, maximum: usize) -> Self {
        self.inner = self.inner.maximum_size(maximum);
        self
    }

    /// Guava's `maximumWeight`.
    pub fn maximum_weight(mut self, maximum: u64) -> Self {
        self.inner = self.inner.maximum_weight(maximum);
        self
    }

    /// Guava's `weigher`.
    pub fn weigher<F>(mut self, weigher: F) -> Self
    where
        F: Fn(&K, &V) -> u32 + Send + Sync + 'static,
    {
        self.inner = self.inner.weigher(weigher);
        self
    }

    /// Guava's `expireAfterWrite`.
    pub fn expire_after_write(mut self, duration: Duration) -> Self {
        self.inner = self.inner.expire_after_write(duration);
        self
    }

    /// Guava's `expireAfterAccess`.
    pub fn expire_after_access(mut self, duration: Duration) -> Self {
        self.inner = self.inner.expire_after_access(duration);
        self
    }

    /// Guava's variable expiration.
    pub fn expire_after<E>(mut self, expiry: E) -> Self
    where
        E: Expiry<K, V>,
    {
        self.inner = self.inner.expire_after(expiry);
        self
    }

    /// Guava's `refreshAfterWrite`.
    pub fn refresh_after_write(mut self, duration: Duration) -> Self {
        self.inner = self.inner.refresh_after_write(duration);
        self
    }

    /// Guava's `recordStats`.
    pub fn record_stats(mut self, enabled: bool) -> Self {
        self.inner = self.inner.record_stats(enabled);
        self
    }

    /// Guava's `removalListener`.
    pub fn removal_listener<F>(mut self, listener: F) -> Self
    where
        F: Fn(&K, &V, RemovalCause) + Send + Sync + 'static,
    {
        self.inner = self.inner.removal_listener(listener);
        self
    }

    /// Caffeine's eviction listener.
    pub fn eviction_listener<F>(mut self, listener: F) -> Self
    where
        F: Fn(&K, &V, RemovalCause) + Send + Sync + 'static,
    {
        self.inner = self.inner.eviction_listener(listener);
        self
    }

    /// Guava's `ticker`.
    pub fn ticker<T: Ticker>(mut self, ticker: T) -> Self {
        self.inner = self.inner.ticker(ticker);
        self
    }

    /// Builds the cache.
    pub fn build(self) -> GuavaCache<K, V> {
        GuavaCache {
            inner: self.inner.build(),
        }
    }

    /// Builds a loading cache.
    pub fn build_with_loader<F>(self, loader: F) -> GuavaCache<K, V>
    where
        F: Fn(&K) -> Option<V> + Send + Sync + 'static,
    {
        GuavaCache {
            inner: self.inner.build_with_loader(loader),
        }
    }
}

impl<K, V> Default for GuavaCacheBuilder<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
