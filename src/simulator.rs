//! Trace-driven policy simulation, mirroring Caffeine's `simulator` module.
//!
//! The simulator replays a key access trace against a chosen policy and
//! reports hits, misses, and evictions. It is intended for comparing policies
//! on a recorded workload, not for production serving.
//!
//! ```
//! use caffeine::simulator::{simulate, Policy};
//!
//! let trace = [1, 2, 1, 3, 1, 2];
//! let stats = simulate(trace, Policy::Lru, 2);
//! assert_eq!(stats.request_count(), 6);
//! assert!(stats.hit_rate() > 0.0);
//! ```

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use crate::cache::Cache;

/// The simulated eviction policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// This crate's Window TinyLFU policy.
    Wtinylfu,
    /// A strict least-recently-used policy.
    Lru,
    /// A least-frequently-used policy, breaking ties by recency.
    Lfu,
}

/// Results of replaying a trace.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PolicyStats {
    /// Lookups that found a live entry.
    pub hits: u64,
    /// Lookups that did not find a live entry.
    pub misses: u64,
    /// Entries evicted by the policy.
    pub evictions: u64,
}

impl PolicyStats {
    /// Total number of lookups.
    pub fn request_count(&self) -> u64 {
        self.hits + self.misses
    }

    /// Ratio of hits to lookups, or `1.0` for an empty trace.
    pub fn hit_rate(&self) -> f64 {
        if self.request_count() == 0 {
            1.0
        } else {
            self.hits as f64 / self.request_count() as f64
        }
    }

    /// Ratio of misses to lookups.
    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }
}

/// Replays `trace` against `policy` with the given capacity.
pub fn simulate(
    trace: impl IntoIterator<Item = u64>,
    policy: Policy,
    maximum_size: usize,
) -> PolicyStats {
    match policy {
        Policy::Wtinylfu => simulate_wtinylfu(trace, maximum_size),
        Policy::Lru => simulate_lru(trace, maximum_size),
        Policy::Lfu => simulate_lfu(trace, maximum_size),
    }
}

fn simulate_wtinylfu(trace: impl IntoIterator<Item = u64>, maximum_size: usize) -> PolicyStats {
    let cache: Cache<u64, ()> = Cache::builder()
        .maximum_size(maximum_size)
        .record_stats(true)
        .build();
    for key in trace {
        if cache.get(&key).is_none() {
            cache.insert(key, ());
        }
    }
    let stats = cache.stats();
    PolicyStats {
        hits: stats.hit_count,
        misses: stats.miss_count,
        evictions: stats.eviction_count,
    }
}

fn simulate_lru(trace: impl IntoIterator<Item = u64>, maximum_size: usize) -> PolicyStats {
    let mut lru = Lru::new(maximum_size);
    let mut stats = PolicyStats::default();
    for key in trace {
        if lru.get(key) {
            stats.hits += 1;
        } else {
            stats.misses += 1;
            if lru.insert(key) {
                stats.evictions += 1;
            }
        }
    }
    stats
}

fn simulate_lfu(trace: impl IntoIterator<Item = u64>, maximum_size: usize) -> PolicyStats {
    let mut lfu = Lfu::new(maximum_size);
    let mut stats = PolicyStats::default();
    for key in trace {
        if lfu.get(key) {
            stats.hits += 1;
        } else {
            stats.misses += 1;
            if lfu.insert(key) {
                stats.evictions += 1;
            }
        }
    }
    stats
}

struct Lru {
    maximum_size: usize,
    tick: u64,
    entries: HashMap<u64, u64>,
    order: BinaryHeap<Reverse<(u64, u64)>>,
}

impl Lru {
    fn new(maximum_size: usize) -> Self {
        Self {
            maximum_size,
            tick: 0,
            entries: HashMap::new(),
            order: BinaryHeap::new(),
        }
    }

    fn get(&mut self, key: u64) -> bool {
        if !self.entries.contains_key(&key) {
            return false;
        }
        self.tick += 1;
        self.entries.insert(key, self.tick);
        self.order.push(Reverse((self.tick, key)));
        true
    }

    fn insert(&mut self, key: u64) -> bool {
        if self.maximum_size == 0 {
            return true;
        }
        self.tick += 1;
        self.entries.insert(key, self.tick);
        self.order.push(Reverse((self.tick, key)));
        if self.entries.len() <= self.maximum_size {
            return false;
        }
        self.evict();
        true
    }

    fn evict(&mut self) {
        while let Some(Reverse((tick, key))) = self.order.pop() {
            if self.entries.get(&key) == Some(&tick) {
                self.entries.remove(&key);
                return;
            }
        }
    }
}

struct Lfu {
    maximum_size: usize,
    tick: u64,
    entries: HashMap<u64, (u64, u64)>,
    order: BinaryHeap<Reverse<(u64, u64, u64)>>,
}

impl Lfu {
    fn new(maximum_size: usize) -> Self {
        Self {
            maximum_size,
            tick: 0,
            entries: HashMap::new(),
            order: BinaryHeap::new(),
        }
    }

    fn get(&mut self, key: u64) -> bool {
        match self.entries.get_mut(&key) {
            Some((count, last)) => {
                *count += 1;
                self.tick += 1;
                *last = self.tick;
                let count = *count;
                self.order.push(Reverse((count, self.tick, key)));
                true
            }
            None => false,
        }
    }

    fn insert(&mut self, key: u64) -> bool {
        if self.maximum_size == 0 {
            return true;
        }
        self.tick += 1;
        self.entries.insert(key, (1, self.tick));
        self.order.push(Reverse((1, self.tick, key)));
        if self.entries.len() <= self.maximum_size {
            return false;
        }
        self.evict();
        true
    }

    fn evict(&mut self) {
        while let Some(Reverse((count, last, key))) = self.order.pop() {
            if self.entries.get(&key) == Some(&(count, last)) {
                self.entries.remove(&key);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_replays_a_small_trace() {
        let stats = simulate([1, 2, 1, 3, 1], Policy::Lru, 2);
        assert_eq!(stats.request_count(), 5);
        assert_eq!(stats.evictions, 1);
        assert!(stats.hit_rate() > 0.0);
    }

    #[test]
    fn an_empty_trace_has_a_perfect_hit_rate() {
        let stats = simulate([], Policy::Wtinylfu, 10);
        assert_eq!(stats.request_count(), 0);
        assert_eq!(stats.hit_rate(), 1.0);
    }

    #[test]
    fn a_zero_capacity_cache_never_hits() {
        let stats = simulate([1, 2, 3], Policy::Wtinylfu, 0);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 3);
        assert_eq!(stats.evictions, 3);
    }
}
