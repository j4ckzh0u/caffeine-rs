//! Immutable cache configuration shared by all shards.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::expiry::Expiration;
use crate::removal::{EvictionListener, RemovalListener};
use crate::ticker::SharedTicker;

/// Sentinel meaning that refresh-after-write is disabled.
pub(crate) const NO_REFRESH: u64 = u64::MAX;

pub(crate) type Loader<K, V> = Arc<dyn Fn(&K) -> Option<V> + Send + Sync + 'static>;
pub(crate) type Executor = Arc<dyn Fn(Box<dyn FnOnce() + Send + 'static>) + Send + Sync + 'static>;
pub(crate) type Weigher<K, V> = Arc<dyn Fn(&K, &V) -> u32 + Send + Sync + 'static>;

pub(crate) struct Config<K, V> {
    pub(crate) max_capacity: u64,
    pub(crate) initial_capacity: usize,
    pub(crate) is_weighted: bool,
    pub(crate) bounded: bool,
    pub(crate) expiration: Expiration<K, V>,
    pub(crate) refresh_after_write: AtomicU64,
    pub(crate) record_stats: bool,
    pub(crate) ticker: SharedTicker,
    pub(crate) weigher: Option<Weigher<K, V>>,
    pub(crate) loader: Option<Loader<K, V>>,
    pub(crate) executor: Executor,
    pub(crate) removal_listener: Option<RemovalListener<K, V>>,
    pub(crate) eviction_listener: Option<EvictionListener<K, V>>,
}

impl<K, V> Config<K, V> {
    pub(crate) fn weight(&self, key: &K, value: &V) -> u32 {
        match &self.weigher {
            Some(weigher) => weigher(key, value),
            None => 1,
        }
    }

    pub(crate) fn now(&self) -> u64 {
        self.ticker.read()
    }

    pub(crate) fn refresh_ttl(&self) -> Option<u64> {
        if self.loader.is_some() {
            let ttl = self.refresh_after_write.load(Ordering::Relaxed);
            (ttl != NO_REFRESH).then_some(ttl)
        } else {
            None
        }
    }

    pub(crate) fn set_refresh_ttl(&self, ttl: Option<u64>) {
        let value = ttl.unwrap_or(NO_REFRESH);
        self.refresh_after_write.store(value, Ordering::Relaxed);
    }
}

pub(crate) fn default_executor(task: Box<dyn FnOnce() + Send + 'static>) {
    let _ = std::thread::Builder::new()
        .name("caffeine-refresh".to_string())
        .spawn(task);
}
