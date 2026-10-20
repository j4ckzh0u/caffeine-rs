//! Removal notifications.

use std::sync::Arc;

/// The reason an entry was removed from the cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RemovalCause {
    /// The entry was manually removed by the caller.
    Explicit,
    /// The entry's value was replaced by a new value.
    Replaced,
    /// The entry's time-to-live elapsed.
    Expired,
    /// The entry was evicted by the size or weight policy.
    Size,
    /// The entry's key or value was garbage-collected (weak/soft references).
    Collected,
}

impl RemovalCause {
    /// Returns `true` for removals the cache performed on the caller's behalf
    /// due to size, weight, expiration, or reference collection. Manual
    /// removal and replacement are not evictions.
    pub fn was_evicted(self) -> bool {
        matches!(
            self,
            RemovalCause::Size | RemovalCause::Expired | RemovalCause::Collected
        )
    }
}

/// Called for every entry removed from the cache, in the removing thread.
pub type RemovalListener<K, V> = Arc<dyn Fn(&K, &V, RemovalCause) + Send + Sync + 'static>;

/// Called for entries evicted by the size/weight policy or by expiration.
pub type EvictionListener<K, V> = Arc<dyn Fn(&K, &V, RemovalCause) + Send + Sync + 'static>;

pub(crate) struct RemovedEntry<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) cause: RemovalCause,
    pub(crate) weight: u32,
}
