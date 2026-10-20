//! A JSR-107-shaped facade, mirroring Caffeine's `jcache` module.
//!
//! JSR-107 is a Java API and has no binary-compatible Rust equivalent. This
//! module offers the same vocabulary — provider, manager, configuration, and
//! cache — backed by this crate's synchronous cache. It is intended for ports
//! that want a familiar surface, not for interoperating with Java providers.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::cache::Cache;
use crate::stats::CacheStats;

/// Immutable view of a cache entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry<K, V> {
    /// The entry's key.
    pub key: K,
    /// The entry's value.
    pub value: V,
}

/// JSR-107-style cache configuration.
pub struct Configuration<K, V> {
    store_by_value: bool,
    read_through: bool,
    write_through: bool,
    statistics: bool,
    marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Default for Configuration<K, V> {
    fn default() -> Self {
        Self {
            store_by_value: false,
            read_through: false,
            write_through: false,
            statistics: false,
            marker: PhantomData,
        }
    }
}

impl<K, V> Configuration<K, V> {
    /// Creates a configuration with JSR-107 defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether keys and values are stored by value.
    pub fn set_store_by_value(mut self, enabled: bool) -> Self {
        self.store_by_value = enabled;
        self
    }

    /// Sets whether reads may be served through a loader.
    pub fn set_read_through(mut self, enabled: bool) -> Self {
        self.read_through = enabled;
        self
    }

    /// Sets whether writes propagate to a backing store.
    pub fn set_write_through(mut self, enabled: bool) -> Self {
        self.write_through = enabled;
        self
    }

    /// Sets whether statistics are collected.
    pub fn set_statistics(mut self, enabled: bool) -> Self {
        self.statistics = enabled;
        self
    }

    /// Returns whether values are stored by value.
    pub fn is_store_by_value(&self) -> bool {
        self.store_by_value
    }

    /// Returns whether read-through is enabled.
    pub fn is_read_through(&self) -> bool {
        self.read_through
    }

    /// Returns whether write-through is enabled.
    pub fn is_write_through(&self) -> bool {
        self.write_through
    }

    /// Returns whether statistics are enabled.
    pub fn is_statistics_enabled(&self) -> bool {
        self.statistics
    }
}

/// Mutable alias for [`Configuration`].
pub type MutableConfiguration<K, V> = Configuration<K, V>;

/// A JSR-107-shaped cache backed by [`Cache`].
pub struct JCache<K, V> {
    inner: Cache<K, V>,
    configuration: Configuration<K, V>,
    closed: AtomicBool,
}

impl<K, V> JCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Wraps a cache with a JSR-107 configuration.
    pub fn new(cache: Cache<K, V>, configuration: Configuration<K, V>) -> Self {
        Self {
            inner: cache,
            configuration,
            closed: AtomicBool::new(false),
        }
    }

    /// Returns the configuration.
    pub fn configuration(&self) -> &Configuration<K, V> {
        &self.configuration
    }

    /// Returns the wrapped cache.
    pub fn unwrap(&self) -> &Cache<K, V> {
        &self.inner
    }

    /// Returns the value or `None`.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.check_open();
        self.inner.get(key)
    }

    /// Returns all entries whose keys were found.
    pub fn get_all<Q>(&self, keys: &[&Q]) -> Vec<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.check_open();
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

    /// Returns `true` when a live entry exists.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.check_open();
        self.inner.contains_key(key)
    }

    /// Inserts or replaces an entry.
    pub fn put(&self, key: K, value: V) {
        self.check_open();
        self.inner.insert(key, value);
    }

    /// Inserts every entry.
    pub fn put_all(&self, entries: impl IntoIterator<Item = (K, V)>) {
        self.check_open();
        for (key, value) in entries {
            self.inner.insert(key, value);
        }
    }

    /// Inserts only when absent.
    pub fn put_if_absent(&self, key: K, value: V) -> Option<V> {
        self.check_open();
        self.inner.put_if_absent(key, value)
    }

    /// Removes an entry.
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.check_open();
        self.inner.invalidate(key)
    }

    /// JSR-107 `loadAll`, loading missing keys with the cache's configured
    /// loader.
    ///
    /// # Panics
    ///
    /// Panics when the underlying cache has no loader.
    pub fn load_all(&self, keys: impl IntoIterator<Item = K>) {
        self.check_open();
        for key in keys {
            self.inner.get_loaded(key);
        }
    }

    /// Removes the listed entries.
    pub fn remove_all<Q>(&self, keys: &[&Q])
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.check_open();
        for key in keys {
            self.inner.invalidate(*key);
        }
    }

    /// Removes every entry.
    pub fn clear(&self) {
        self.check_open();
        self.inner.invalidate_all();
    }

    /// Number of entries.
    pub fn size(&self) -> usize {
        self.check_open();
        self.inner.len()
    }

    /// Clones every key.
    pub fn keys(&self) -> Vec<K> {
        self.check_open();
        self.inner.keys()
    }

    /// Clones every entry.
    pub fn entries(&self) -> Vec<Entry<K, V>> {
        self.check_open();
        self.inner
            .iter()
            .map(|(key, value)| Entry { key, value })
            .collect()
    }

    /// Statistics snapshot.
    pub fn statistics(&self) -> CacheStats {
        self.inner.stats()
    }

    /// Returns `true` after [`close`](Self::close).
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Marks the cache closed. Later operations panic, like JSR-107's
    /// `IllegalStateException`.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }

    fn check_open(&self) {
        assert!(!self.is_closed(), "cache is closed");
    }
}

/// A named-cache registry.
#[derive(Default)]
pub struct CacheManager {
    caches: Mutex<HashMap<String, Arc<dyn std::any::Any + Send + Sync>>>,
}

impl CacheManager {
    /// Creates an empty manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates and registers a cache under `name`.
    pub fn create_cache<K, V>(
        &self,
        name: impl Into<String>,
        configuration: Configuration<K, V>,
        cache: Cache<K, V>,
    ) -> Arc<JCache<K, V>>
    where
        K: Hash + Eq + Clone + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        let cache = Arc::new(JCache::new(cache, configuration));
        lock(&self.caches).insert(name.into(), cache.clone());
        cache
    }

    /// Looks up a typed cache by name.
    pub fn get_cache<K, V>(&self, name: &str) -> Option<Arc<JCache<K, V>>>
    where
        K: Hash + Eq + Clone + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        let cache = lock(&self.caches).get(name).cloned()?;
        cache.downcast::<JCache<K, V>>().ok()
    }

    /// Removes a cache from the registry.
    pub fn destroy_cache(&self, name: &str) -> bool {
        lock(&self.caches).remove(name).is_some()
    }

    /// Names of all registered caches.
    pub fn cache_names(&self) -> Vec<String> {
        lock(&self.caches).keys().cloned().collect()
    }

    /// Drops every registered cache.
    pub fn close(&self) {
        lock(&self.caches).clear();
    }
}

/// A JSR-107-shaped provider wrapping a [`CacheManager`].
#[derive(Default)]
pub struct CachingProvider {
    manager: CacheManager,
}

impl CachingProvider {
    /// Creates a provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the manager.
    pub fn get_cache_manager(&self) -> &CacheManager {
        &self.manager
    }

    /// Closes every cache known to the manager.
    pub fn close(&self) {
        self.manager.close();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
