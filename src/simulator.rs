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
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

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
    /// First in, first out.
    Fifo,
    /// A segmented LRU with protected and probation regions.
    Slru,
    /// The CLOCK second-chance approximation of LRU.
    Clock,
    /// Belady's optimal policy, using knowledge of future accesses.
    Optimal,
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
        Policy::Fifo => simulate_fifo(trace, maximum_size),
        Policy::Slru => simulate_slru(trace, maximum_size),
        Policy::Clock => simulate_clock(trace, maximum_size),
        Policy::Optimal => simulate_optimal(trace, maximum_size),
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

fn simulate_fifo(trace: impl IntoIterator<Item = u64>, maximum_size: usize) -> PolicyStats {
    let mut order = VecDeque::new();
    let mut entries = HashSet::new();
    let mut stats = PolicyStats::default();
    for key in trace {
        if entries.contains(&key) {
            stats.hits += 1;
            continue;
        }
        stats.misses += 1;
        if maximum_size == 0 {
            stats.evictions += 1;
            continue;
        }
        if entries.len() >= maximum_size {
            if let Some(victim) = order.pop_front() {
                entries.remove(&victim);
                stats.evictions += 1;
            }
        }
        entries.insert(key);
        order.push_back(key);
    }
    stats
}

fn simulate_slru(trace: impl IntoIterator<Item = u64>, maximum_size: usize) -> PolicyStats {
    let protected_limit = maximum_size * 4 / 5;
    let probation_limit = maximum_size - protected_limit;
    let mut probation: HashMap<u64, u64> = HashMap::new();
    let mut protected: HashMap<u64, u64> = HashMap::new();
    let mut probation_order = BinaryHeap::new();
    let mut protected_order = BinaryHeap::new();
    let mut tick = 0u64;
    let mut stats = PolicyStats::default();

    for key in trace {
        tick += 1;
        if let Some(last) = protected.get_mut(&key) {
            *last = tick;
            protected_order.push(Reverse((tick, key)));
            stats.hits += 1;
            continue;
        }
        if probation.remove(&key).is_some() {
            stats.hits += 1;
            protected.insert(key, tick);
            protected_order.push(Reverse((tick, key)));
            if protected.len() > protected_limit {
                if let Some(victim) = pop_lru(&mut protected_order, &protected) {
                    protected.remove(&victim);
                    probation.insert(victim, tick);
                    probation_order.push(Reverse((tick, victim)));
                    if probation.len() > probation_limit {
                        if let Some(evicted) = pop_lru(&mut probation_order, &probation) {
                            probation.remove(&evicted);
                            stats.evictions += 1;
                        }
                    }
                }
            }
            continue;
        }

        stats.misses += 1;
        if maximum_size == 0 {
            stats.evictions += 1;
            continue;
        }
        probation.insert(key, tick);
        probation_order.push(Reverse((tick, key)));
        if probation.len() > probation_limit {
            if let Some(evicted) = pop_lru(&mut probation_order, &probation) {
                probation.remove(&evicted);
                stats.evictions += 1;
            }
        }
    }
    stats
}

fn pop_lru(
    order: &mut BinaryHeap<Reverse<(u64, u64)>>,
    entries: &HashMap<u64, u64>,
) -> Option<u64> {
    while let Some(Reverse((tick, key))) = order.pop() {
        if entries.get(&key) == Some(&tick) {
            return Some(key);
        }
    }
    None
}

fn simulate_clock(trace: impl IntoIterator<Item = u64>, maximum_size: usize) -> PolicyStats {
    struct Slot {
        key: u64,
        referenced: bool,
    }

    let mut slots: Vec<Slot> = Vec::new();
    let mut positions: HashMap<u64, usize> = HashMap::new();
    let mut hand = 0usize;
    let mut stats = PolicyStats::default();

    for key in trace {
        if let Some(&index) = positions.get(&key) {
            slots[index].referenced = true;
            stats.hits += 1;
            continue;
        }
        stats.misses += 1;
        if maximum_size == 0 {
            stats.evictions += 1;
            continue;
        }
        if slots.len() < maximum_size {
            positions.insert(key, slots.len());
            slots.push(Slot {
                key,
                referenced: true,
            });
            continue;
        }
        loop {
            let index = hand;
            hand = (hand + 1) % slots.len();
            if slots[index].referenced {
                slots[index].referenced = false;
            } else {
                positions.remove(&slots[index].key);
                positions.insert(key, index);
                slots[index] = Slot {
                    key,
                    referenced: true,
                };
                stats.evictions += 1;
                break;
            }
        }
    }
    stats
}

fn simulate_optimal(trace: impl IntoIterator<Item = u64>, maximum_size: usize) -> PolicyStats {
    let trace: Vec<u64> = trace.into_iter().collect();
    let mut future: HashMap<u64, VecDeque<usize>> = HashMap::new();
    for (index, &key) in trace.iter().enumerate() {
        future.entry(key).or_default().push_back(index);
    }
    let mut cached = HashSet::new();
    let mut stats = PolicyStats::default();

    for (index, &key) in trace.iter().enumerate() {
        if let Some(queue) = future.get_mut(&key) {
            if queue.front() == Some(&index) {
                queue.pop_front();
            }
        }
        if cached.contains(&key) {
            stats.hits += 1;
            continue;
        }
        stats.misses += 1;
        if maximum_size == 0 {
            stats.evictions += 1;
            continue;
        }
        if cached.len() >= maximum_size {
            let victim = cached
                .iter()
                .max_by_key(|candidate| {
                    future
                        .get(candidate)
                        .and_then(|queue| queue.front().copied())
                        .unwrap_or(usize::MAX)
                })
                .copied();
            if let Some(victim) = victim {
                cached.remove(&victim);
                stats.evictions += 1;
            }
        }
        cached.insert(key);
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

#[cfg(test)]
mod extended_tests {
    use super::*;

    #[test]
    fn classic_policies_replay_the_same_trace() {
        let fifo = simulate([1, 2, 1, 3, 1], Policy::Fifo, 2);
        assert_eq!(fifo.request_count(), 5);
        assert_eq!(fifo.hits, 1);
        assert_eq!(fifo.evictions, 2);

        let clock = simulate([1, 2, 1, 3, 1], Policy::Clock, 2);
        assert_eq!(clock.request_count(), 5);
        assert_eq!(clock.hits, 1);

        let slru = simulate([1, 2, 1, 3, 1], Policy::Slru, 2);
        assert_eq!(slru.request_count(), 5);

        let optimal = simulate([1, 2, 3, 1, 2, 4, 1, 2, 3, 4], Policy::Optimal, 3);
        assert_eq!(optimal.request_count(), 10);
        assert_eq!(optimal.hits, 5);
        assert_eq!(optimal.evictions, 2);
    }

    #[test]
    fn zero_capacity_never_hits_for_new_policies() {
        for policy in [Policy::Fifo, Policy::Slru, Policy::Clock, Policy::Optimal] {
            let stats = simulate([1, 2, 3], policy, 0);
            assert_eq!(stats.hits, 0);
            assert_eq!(stats.misses, 3);
            assert_eq!(stats.evictions, 3);
        }
    }
}
