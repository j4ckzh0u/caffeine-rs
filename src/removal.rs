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
}

impl RemovalCause {
    /// Returns `true` if this removal was an eviction by the policy.
    pub fn was_evicted(self) -> bool {
        matches!(self, RemovalCause::Size)
    }
}

/// Called for every entry removed from the cache, in the removing thread.
pub type RemovalListener<K, V> = Arc<dyn Fn(&K, &V, RemovalCause) + Send + Sync + 'static>;

/// Called only for entries evicted by the size or weight policy.
pub type EvictionListener<K, V> = Arc<dyn Fn(&K, &V, RemovalCause) + Send + Sync + 'static>;

pub(crate) struct RemovedEntry<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) cause: RemovalCause,
    pub(crate) weight: u32,
}
