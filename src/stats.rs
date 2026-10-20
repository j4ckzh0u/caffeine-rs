//! Cache access statistics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default, Debug)]
pub(crate) struct StatsCounters {
    pub(crate) hit: AtomicU64,
    pub(crate) miss: AtomicU64,
    pub(crate) load_success: AtomicU64,
    pub(crate) load_failure: AtomicU64,
    pub(crate) total_load_nanos: AtomicU64,
    pub(crate) evictions: AtomicU64,
    pub(crate) evicted_weight: AtomicU64,
}

impl StatsCounters {
    pub(crate) fn snapshot(&self, recording: bool) -> CacheStats {
        if !recording {
            return CacheStats::default();
        }
        CacheStats {
            hit_count: self.hit.load(Ordering::Relaxed),
            miss_count: self.miss.load(Ordering::Relaxed),
            load_success_count: self.load_success.load(Ordering::Relaxed),
            load_failure_count: self.load_failure.load(Ordering::Relaxed),
            total_load_time: Duration::from_nanos(self.total_load_nanos.load(Ordering::Relaxed)),
            eviction_count: self.evictions.load(Ordering::Relaxed),
            evicted_weight: self.evicted_weight.load(Ordering::Relaxed),
        }
    }
}

/// A snapshot of the cache's access statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Number of cache lookups that found a live entry.
    pub hit_count: u64,
    /// Number of cache lookups that did not find a live entry.
    pub miss_count: u64,
    /// Number of successful loads, including refreshes.
    pub load_success_count: u64,
    /// Number of failed loads, including refreshes.
    pub load_failure_count: u64,
    /// Total time spent in successful loads.
    pub total_load_time: Duration,
    /// Number of entries evicted by the size/weight policy or expiration.
    pub eviction_count: u64,
    /// Total weight of evicted entries.
    pub evicted_weight: u64,
}

impl CacheStats {
    /// Total number of lookups (`hit_count + miss_count`).
    pub fn request_count(&self) -> u64 {
        self.hit_count + self.miss_count
    }

    /// Ratio of hits to lookups, or `1.0` when there were no lookups.
    pub fn hit_rate(&self) -> f64 {
        if self.request_count() == 0 {
            1.0
        } else {
            self.hit_count as f64 / self.request_count() as f64
        }
    }

    /// Ratio of misses to lookups, or `0.0` when there were no lookups.
    pub fn miss_rate(&self) -> f64 {
        if self.request_count() == 0 {
            0.0
        } else {
            self.miss_count as f64 / self.request_count() as f64
        }
    }

    /// Total number of loads (`load_success_count + load_failure_count`).
    pub fn load_count(&self) -> u64 {
        self.load_success_count + self.load_failure_count
    }

    /// Average time spent in a successful load, or zero when there were none.
    pub fn average_load_penalty(&self) -> Duration {
        let nanos = self.total_load_time.as_nanos() as u64;
        Duration::from_nanos(nanos.checked_div(self.load_success_count).unwrap_or(0))
    }
}
