//! Fluent cache construction, mirroring Caffeine's `Caffeine` builder.

use std::hash::Hash;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::Cache;
use crate::config::{default_executor, Config, Executor, Loader, Weigher};
use crate::expiry::{Expiration, Expiry};
use crate::removal::{EvictionListener, RemovalCause, RemovalListener};
use crate::ticker::{nanos, SharedTicker, SystemTicker, Ticker};

/// Builds a [`Cache`].
///
/// # Panics
///
/// [`build`](Self::build) panics on invalid combinations:
/// - both `maximum_size` and `maximum_weight`
/// - `maximum_weight` without a `weigher`, or a `weigher` without `maximum_weight`
/// - `expire_after` combined with `expire_after_write` or `expire_after_access`
/// - `refresh_after_write` without a loader
pub struct CacheBuilder<K, V> {
    maximum_size: Option<usize>,
    maximum_weight: Option<u64>,
    weigher: Option<Weigher<K, V>>,
    initial_capacity: usize,
    expire_after_write: Option<Duration>,
    expire_after_access: Option<Duration>,
    expiry: Option<Arc<dyn Expiry<K, V>>>,
    refresh_after_write: Option<Duration>,
    record_stats: bool,
    ticker: Option<SharedTicker>,
    loader: Option<Loader<K, V>>,
    executor: Option<Executor>,
    removal_listener: Option<RemovalListener<K, V>>,
    eviction_listener: Option<EvictionListener<K, V>>,
}

impl<K, V> CacheBuilder<K, V> {
    /// Creates a builder for an unbounded cache.
    pub fn new() -> Self {
        Self {
            maximum_size: None,
            maximum_weight: None,
            weigher: None,
            initial_capacity: 0,
            expire_after_write: None,
            expire_after_access: None,
            expiry: None,
            refresh_after_write: None,
            record_stats: false,
            ticker: None,
            loader: None,
            executor: None,
            removal_listener: None,
            eviction_listener: None,
        }
    }

    /// Evicts entries so the cache holds at most `maximum` entries.
    pub fn maximum_size(mut self, maximum: usize) -> Self {
        self.maximum_size = Some(maximum);
        self
    }

    /// Evicts entries so their total weight does not exceed `maximum`.
    /// Requires a [`weigher`](Self::weigher).
    pub fn maximum_weight(mut self, maximum: u64) -> Self {
        self.maximum_weight = Some(maximum);
        self
    }

    /// Estimates the initial table size to avoid rehashing.
    pub fn initial_capacity(mut self, initial_capacity: usize) -> Self {
        self.initial_capacity = initial_capacity;
        self
    }

    /// Expires an entry `duration` after its last write (insert or replace).
    ///
    /// May be combined with [`expire_after_access`](Self::expire_after_access);
    /// the entry then expires when either timeout fires first.
    pub fn expire_after_write(mut self, duration: Duration) -> Self {
        self.expire_after_write = Some(duration);
        self
    }

    /// Expires an entry `duration` after its last read or write.
    ///
    /// May be combined with [`expire_after_write`](Self::expire_after_write);
    /// the entry then expires when either timeout fires first. Mutually
    /// exclusive with [`expire_after`](Self::expire_after).
    pub fn expire_after_access(mut self, duration: Duration) -> Self {
        self.expire_after_access = Some(duration);
        self
    }

    /// Uses a custom per-entry expiration policy.
    ///
    /// Mutually exclusive with [`expire_after_write`](Self::expire_after_write)
    /// and [`expire_after_access`](Self::expire_after_access). See
    /// [`Expiry`] and its helper types.
    pub fn expire_after<E>(mut self, expiry: E) -> Self
    where
        E: Expiry<K, V>,
    {
        self.expiry = Some(Arc::new(expiry));
        self
    }

    /// Makes an entry eligible for an asynchronous reload `duration` after
    /// its last write. A stale read returns the old value and schedules one
    /// loader call per key.
    ///
    /// Requires a loader: build with [`build_with_loader`](Self::build_with_loader).
    pub fn refresh_after_write(mut self, duration: Duration) -> Self {
        self.refresh_after_write = Some(duration);
        self
    }

    /// Enables collection of hit, miss, load, and eviction statistics.
    pub fn record_stats(mut self, enabled: bool) -> Self {
        self.record_stats = enabled;
        self
    }

    /// Uses a custom, monotonically increasing time source.
    pub fn ticker<T: Ticker>(mut self, ticker: T) -> Self {
        self.ticker = Some(Arc::new(ticker));
        self
    }

    /// Uses a custom executor for asynchronous refreshes.
    pub fn executor<F>(mut self, executor: F) -> Self
    where
        F: Fn(Box<dyn FnOnce() + Send + 'static>) + Send + Sync + 'static,
    {
        self.executor = Some(Arc::new(executor));
        self
    }

    /// Computes an entry's weight. Only valid with
    /// [`maximum_weight`](Self::maximum_weight).
    pub fn weigher<F>(mut self, weigher: F) -> Self
    where
        F: Fn(&K, &V) -> u32 + Send + Sync + 'static,
    {
        self.weigher = Some(Arc::new(weigher));
        self
    }

    /// Called for every entry removed from the cache, in the removing thread.
    pub fn removal_listener<F>(mut self, listener: F) -> Self
    where
        F: Fn(&K, &V, RemovalCause) + Send + Sync + 'static,
    {
        self.removal_listener = Some(Arc::new(listener));
        self
    }

    /// Called for entries evicted by the size/weight policy or expiration.
    pub fn eviction_listener<F>(mut self, listener: F) -> Self
    where
        F: Fn(&K, &V, RemovalCause) + Send + Sync + 'static,
    {
        self.eviction_listener = Some(Arc::new(listener));
        self
    }
}

impl<K, V> Default for CacheBuilder<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> CacheBuilder<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Builds a cache without a loader.
    pub fn build(self) -> Cache<K, V> {
        self.finish()
    }

    /// Builds a cache whose refresh operation calls `loader`.
    ///
    /// A loader returning `None` (or panicking) keeps the previous value.
    pub fn build_with_loader<F>(mut self, loader: F) -> Cache<K, V>
    where
        F: Fn(&K) -> Option<V> + Send + Sync + 'static,
    {
        self.loader = Some(Arc::new(loader));
        self.finish()
    }

    fn finish(self) -> Cache<K, V> {
        if self.maximum_size.is_some() && self.maximum_weight.is_some() {
            panic!("maximum_size and maximum_weight are mutually exclusive");
        }
        if self.maximum_weight.is_some() && self.weigher.is_none() {
            panic!("maximum_weight requires a weigher");
        }
        if self.weigher.is_some() && self.maximum_weight.is_none() {
            panic!("a weigher requires maximum_weight");
        }
        if self.expiry.is_some()
            && (self.expire_after_access.is_some() || self.expire_after_write.is_some())
        {
            panic!("expire_after is mutually exclusive with the fixed expiration methods");
        }
        if self.refresh_after_write.is_some() && self.loader.is_none() {
            panic!("refresh_after_write requires a loader; use build_with_loader");
        }

        let max_capacity = match (self.maximum_size, self.maximum_weight) {
            (Some(size), None) => size as u64,
            (None, Some(weight)) => weight,
            (None, None) => u64::MAX / 2,
            (Some(_), Some(_)) => unreachable!("validated above"),
        };
        let bounded = self.maximum_size.is_some() || self.maximum_weight.is_some();
        let expiration = match (
            self.expiry,
            self.expire_after_write,
            self.expire_after_access,
        ) {
            (Some(expiry), None, None) => Expiration::Custom(expiry),
            (None, Some(write), Some(access)) => Expiration::Both {
                write: nanos(write),
                access: nanos(access),
            },
            (None, Some(duration), None) => Expiration::AfterWrite(nanos(duration)),
            (None, None, Some(duration)) => Expiration::AfterAccess(nanos(duration)),
            _ => Expiration::None,
        };

        let config = Config {
            max_capacity,
            initial_capacity: self.initial_capacity,
            is_weighted: self.maximum_weight.is_some(),
            bounded,
            expiration,
            refresh_after_write: AtomicU64::new(
                self.refresh_after_write
                    .map(nanos)
                    .unwrap_or(crate::config::NO_REFRESH),
            ),
            record_stats: self.record_stats,
            ticker: self.ticker.unwrap_or_else(|| Arc::new(SystemTicker::new())),
            weigher: self.weigher,
            loader: self.loader,
            executor: self
                .executor
                .unwrap_or_else(|| Arc::new(default_executor) as Executor),
            removal_listener: self.removal_listener,
            eviction_listener: self.eviction_listener,
        };
        Cache::new(config)
    }
}
