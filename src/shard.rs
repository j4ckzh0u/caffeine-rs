//! The cache's data and eviction policy: hash index + intrusive LRU lists +
//! W-TinyLFU. A single shard holds the whole cache so the admission window,
//! segmented main space, sketch, and climber are global, as in Caffeine.

use std::borrow::Borrow;
use std::cell::Cell;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, Hash, Hasher};

use crate::climber::WindowClimber;
use crate::expiry::{Deadlines, Expiration, MAXIMUM_EXPIRY, NEVER};
use crate::frequency::FrequencySketch;
use crate::removal::{RemovalCause, RemovedEntry};

pub(crate) const NIL: u32 = u32::MAX;

/// Caffeine bounds the number of entries transferred per maintenance cycle.
const QUEUE_TRANSFER_THRESHOLD: usize = 1_000;

/// A warm candidate may still be admitted against a warm victim at random to
/// blunt frequency-skewing attacks. The frequency counters max out at 15 and
/// are halved on reset, so 6 identifies a warm entry.
const ADMIT_HASHDOS_THRESHOLD: u8 = 6;

thread_local! {
    static ADMIT_RANDOM: Cell<u64> = const { Cell::new(0) };
}

/// Returns whether the window candidate should displace the main victim.
fn admit(candidate: u8, victim: u8) -> bool {
    if candidate > victim {
        return true;
    }
    if candidate < ADMIT_HASHDOS_THRESHOLD {
        return false;
    }
    // One in 128 warm candidates is admitted at random, matching Caffeine's
    // `(random & 127) == 0` hash-DoS guard.
    ADMIT_RANDOM.with(|cell| {
        let mut seed = cell.get();
        if seed == 0 {
            let mut hasher = RandomState::new().build_hasher();
            hasher.write_u64(std::process::id() as u64);
            seed = hasher.finish() | 1;
        }
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        cell.set(seed);
        (seed & 127) == 0
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Region {
    Window,
    Probation,
    Protected,
}

#[derive(Clone, Copy)]
struct LruList {
    head: u32,
    tail: u32,
}

impl Default for LruList {
    fn default() -> Self {
        Self {
            head: NIL,
            tail: NIL,
        }
    }
}

struct Node<K, V> {
    key: K,
    value: V,
    hash: u64,
    weight: u32,
    region: Region,
    write_time: u64,
    /// Last read or write, used by the expiration policy view.
    access_time: u64,
    /// Effective deadline: the earlier of the access and write deadlines.
    expire_at: u64,
    /// Write deadline retained when both fixed TTLs are configured.
    write_expire_at: u64,
    refreshing: bool,
    prev: u32,
    next: u32,
}

pub(crate) struct Hit<K, V> {
    pub(crate) value: V,
    /// The key and write timestamp to capture for an asynchronous refresh.
    pub(crate) refresh: Option<(K, u64)>,
}

pub(crate) struct Shard<K, V> {
    index: HashMap<K, u32>,
    nodes: Vec<Option<Box<Node<K, V>>>>,
    free: Vec<u32>,
    window: LruList,
    probation: LruList,
    protected: LruList,
    /// Ordered deadlines, used to expire everything due without a full scan.
    expiry_index: BTreeMap<(u64, u32), ()>,
    sketch: FrequencySketch,
    climber: WindowClimber,
    capacity: u64,
    global_max_weight: u64,
    bounded: bool,
    window_max: u64,
    protected_max: u64,
    total_weight: u64,
    window_weight: u64,
    probation_weight: u64,
    protected_weight: u64,
    expiration: Expiration<K, V>,
}

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    pub(crate) fn new(
        capacity: u64,
        global_max_weight: u64,
        initial_capacity: usize,
        expiration: Expiration<K, V>,
        bounded: bool,
    ) -> Self {
        let window_max = if capacity == 0 {
            0
        } else {
            (capacity / 100).max(1).min(capacity)
        };
        let main_capacity = capacity.saturating_sub(window_max);
        let protected_max = ((main_capacity as u128 * 4) / 5) as u64;
        Self {
            index: HashMap::with_capacity(initial_capacity),
            nodes: Vec::with_capacity(initial_capacity),
            free: Vec::new(),
            window: LruList::default(),
            probation: LruList::default(),
            protected: LruList::default(),
            expiry_index: BTreeMap::new(),
            sketch: FrequencySketch::new(capacity),
            climber: WindowClimber::new(capacity),
            capacity,
            global_max_weight,
            bounded,
            window_max,
            protected_max,
            total_weight: 0,
            window_weight: 0,
            probation_weight: 0,
            protected_weight: 0,
            expiration,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.index.len()
    }

    pub(crate) fn weighted_size(&self) -> u64 {
        self.total_weight
    }

    pub(crate) fn window_maximum(&self) -> u64 {
        self.window_max
    }

    pub(crate) fn maximum(&self) -> u64 {
        self.global_max_weight
    }

    pub(crate) fn weight_of<Q>(&self, key: &Q, now: u64) -> Option<u32>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let &id = self.index.get(key)?;
        if self.is_expired(id, now) {
            return None;
        }
        Some(self.node_weight(id) as u32)
    }

    pub(crate) fn age_of<Q>(&self, key: &Q, now: u64, access: bool) -> Option<u64>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let &id = self.index.get(key)?;
        if self.is_expired(id, now) {
            return None;
        }
        let node = self.nodes[id as usize].as_ref().expect("live node");
        let timestamp = if access {
            node.access_time
        } else {
            node.write_time
        };
        Some(now.saturating_sub(timestamp))
    }

    pub(crate) fn expires_at<Q>(&self, key: &Q, now: u64) -> Option<u64>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let &id = self.index.get(key)?;
        if self.is_expired(id, now) {
            return None;
        }
        let at = self.nodes[id as usize]
            .as_ref()
            .expect("live node")
            .expire_at;
        (at != NEVER).then_some(at)
    }

    pub(crate) fn refreshable_at<Q>(&self, key: &Q, now: u64, ttl: u64) -> Option<u64>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let &id = self.index.get(key)?;
        if self.is_expired(id, now) {
            return None;
        }
        let write_time = self.nodes[id as usize]
            .as_ref()
            .expect("live node")
            .write_time;
        Some(write_time.saturating_add(ttl))
    }

    /// Changes the maximum size or weight, including the window and protected
    /// quotas, then eagerly evicts any excess.
    pub(crate) fn set_maximum(&mut self, maximum: u64, removed: &mut Vec<RemovedEntry<K, V>>) {
        if maximum == self.global_max_weight {
            return;
        }
        self.global_max_weight = maximum;
        self.capacity = maximum;
        self.window_max = if maximum == 0 {
            0
        } else {
            (maximum / 100).max(1).min(maximum)
        };
        let main = maximum.saturating_sub(self.window_max);
        self.protected_max = ((main as u128 * 4) / 5) as u64;
        self.climber.resized(maximum);
        self.evict(removed);
        self.climb();
    }

    /// Entries ordered from least to most likely to be retained.
    pub(crate) fn coldest(&self, limit: usize) -> Vec<(K, V)> {
        let mut primary = self.collect_region(Region::Window, false);
        primary.extend(self.collect_region(Region::Probation, false));
        primary.sort_by_key(|(_, _, frequency)| *frequency);
        let mut out: Vec<(K, V)> = primary
            .into_iter()
            .map(|(key, value, _)| (key, value))
            .collect();
        out.extend(
            self.collect_region(Region::Protected, false)
                .into_iter()
                .map(|(key, value, _)| (key, value)),
        );
        out.truncate(limit);
        out
    }

    /// Entries ordered from most to least likely to be retained.
    pub(crate) fn hottest(&self, limit: usize) -> Vec<(K, V)> {
        let mut out: Vec<(K, V)> = self
            .collect_region(Region::Protected, true)
            .into_iter()
            .map(|(key, value, _)| (key, value))
            .collect();
        let mut secondary = self.collect_region(Region::Window, true);
        secondary.extend(self.collect_region(Region::Probation, true));
        secondary.sort_by_key(|(_, _, frequency)| std::cmp::Reverse(*frequency));
        out.extend(secondary.into_iter().map(|(key, value, _)| (key, value)));
        out.truncate(limit);
        out
    }

    /// Returns keys currently marked for an asynchronous refresh.
    pub(crate) fn refreshing_keys(&self) -> Vec<K> {
        self.nodes
            .iter()
            .filter_map(|node| node.as_ref())
            .filter(|node| node.refreshing)
            .map(|node| node.key.clone())
            .collect()
    }

    pub(crate) fn fixed_access(&self) -> Option<u64> {
        self.expiration.fixed_access()
    }

    pub(crate) fn fixed_write(&self) -> Option<u64> {
        self.expiration.fixed_write()
    }

    /// Replaces a fixed expiration duration and recomputes every entry's
    /// deadline, matching Caffeine's `Policy.FixedExpiration`.
    pub(crate) fn set_fixed_duration(
        &mut self,
        access: bool,
        duration: u64,
        now: u64,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) {
        match &mut self.expiration {
            Expiration::AfterAccess(current) if access => *current = duration,
            Expiration::AfterWrite(current) if !access => *current = duration,
            Expiration::Both {
                write,
                access: idle,
            } => {
                if access {
                    *idle = duration;
                } else {
                    *write = duration;
                }
            }
            _ => return,
        }
        let ids: Vec<u32> = self.index.values().copied().collect();
        self.expiry_index.clear();
        for id in ids {
            let deadlines = {
                let node = self.nodes[id as usize].as_ref().expect("live node");
                self.fixed_deadlines(node.access_time, node.write_time)
            };
            if let Some(node) = self.nodes[id as usize].as_mut() {
                node.expire_at = deadlines.at;
                node.write_expire_at = deadlines.write_at;
            }
            self.schedule(id, deadlines.at);
        }
        self.expire_entries(now, removed);
    }

    /// Sets a variable deadline of `duration` from now for one entry.
    pub(crate) fn set_expiry<Q>(
        &mut self,
        key: &Q,
        now: u64,
        duration: u64,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let Some(&id) = self.index.get(key) else {
            return;
        };
        let old = self.nodes[id as usize]
            .as_ref()
            .expect("live node")
            .expire_at;
        let at = now.saturating_add(duration).min(MAXIMUM_EXPIRY);
        if let Some(node) = self.nodes[id as usize].as_mut() {
            node.expire_at = at;
            node.write_expire_at = NEVER;
        }
        self.reschedule(id, old, at);
        self.expire_entries(now, removed);
    }

    /// Entries ordered by last access or last write time.
    pub(crate) fn order_by_time(&self, access: bool, oldest: bool, limit: usize) -> Vec<(K, V)> {
        let mut entries: Vec<(u64, K, V)> = self
            .nodes
            .iter()
            .filter_map(|node| node.as_ref())
            .map(|node| {
                let time = if access {
                    node.access_time
                } else {
                    node.write_time
                };
                (time, node.key.clone(), node.value.clone())
            })
            .collect();
        entries.sort_by_key(|(time, _, _)| *time);
        if !oldest {
            entries.reverse();
        }
        entries
            .into_iter()
            .take(limit)
            .map(|(_, key, value)| (key, value))
            .collect()
    }

    /// Entries ordered by their absolute expiration deadline.
    pub(crate) fn order_by_expiry(&self, oldest: bool, limit: usize) -> Vec<(K, V)> {
        let mut entries: Vec<(u64, K, V)> = self
            .nodes
            .iter()
            .filter_map(|node| node.as_ref())
            .filter(|node| node.expire_at != NEVER)
            .map(|node| (node.expire_at, node.key.clone(), node.value.clone()))
            .collect();
        entries.sort_by_key(|(at, _, _)| *at);
        if !oldest {
            entries.reverse();
        }
        entries
            .into_iter()
            .take(limit)
            .map(|(_, key, value)| (key, value))
            .collect()
    }

    fn fixed_deadlines(&self, access_time: u64, write_time: u64) -> Deadlines {
        match &self.expiration {
            Expiration::AfterWrite(duration) => {
                let at = write_time.saturating_add(*duration).min(MAXIMUM_EXPIRY);
                Deadlines { at, write_at: at }
            }
            Expiration::AfterAccess(duration) => Deadlines {
                at: access_time.saturating_add(*duration).min(MAXIMUM_EXPIRY),
                write_at: NEVER,
            },
            Expiration::Both { write, access } => {
                let write_at = write_time.saturating_add(*write).min(MAXIMUM_EXPIRY);
                let idle = access_time.saturating_add(*access).min(MAXIMUM_EXPIRY);
                Deadlines {
                    at: idle.min(write_at),
                    write_at,
                }
            }
            Expiration::None | Expiration::Custom(_) => Deadlines::NEVER,
        }
    }

    fn collect_region(&self, region: Region, from_mru: bool) -> Vec<(K, V, u8)> {
        let mut out = Vec::new();
        let mut id = if from_mru {
            self.list(region).head
        } else {
            self.list(region).tail
        };
        while id != NIL {
            let node = self.nodes[id as usize].as_ref().expect("live node");
            out.push((
                node.key.clone(),
                node.value.clone(),
                self.sketch.frequency(node.hash),
            ));
            id = if from_mru { node.next } else { node.prev };
        }
        out
    }

    pub(crate) fn get<Q>(
        &mut self,
        key: &Q,
        now: u64,
        refresh_ttl: Option<u64>,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) -> Option<Hit<K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.expire_entries(now, removed);
        let &id = self.index.get(key)?;
        if self.is_expired(id, now) {
            self.remove_node(id, RemovalCause::Expired, removed);
            return None;
        }
        let (value, hash, region, refresh, old_deadline, new_deadlines, weight) = {
            let node = self.nodes[id as usize].as_mut().expect("live node");
            let old_deadline = node.expire_at;
            let new_deadlines = self.expiration.on_read(
                &node.key,
                &node.value,
                now,
                Deadlines {
                    at: node.expire_at,
                    write_at: node.write_expire_at,
                },
            );
            let refresh = match refresh_ttl {
                Some(ttl) if !node.refreshing && now.saturating_sub(node.write_time) >= ttl => {
                    node.refreshing = true;
                    Some((node.key.clone(), node.write_time))
                }
                _ => None,
            };
            (
                node.value.clone(),
                node.hash,
                node.region,
                refresh,
                old_deadline,
                new_deadlines,
                node.weight,
            )
        };
        if new_deadlines.at != old_deadline {
            self.reschedule(id, old_deadline, new_deadlines.at);
        }
        if let Some(node) = self.nodes[id as usize].as_mut() {
            node.write_expire_at = new_deadlines.write_at;
            node.access_time = now;
        }
        if weight != 0 {
            self.record_hit(region == Region::Window, region == Region::Probation);
        }
        self.sketch.increment(hash);
        self.touch(region, id);
        self.climb();
        Some(Hit { value, refresh })
    }

    pub(crate) fn peek<Q>(
        &mut self,
        key: &Q,
        now: u64,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.expire_entries(now, removed);
        let id = *self.index.get(key)?;
        if self.is_expired(id, now) {
            self.remove_node(id, RemovalCause::Expired, removed);
            return None;
        }
        Some(
            self.nodes[id as usize]
                .as_ref()
                .expect("live node")
                .value
                .clone(),
        )
    }

    pub(crate) fn contains<Q>(
        &mut self,
        key: &Q,
        now: u64,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.expire_entries(now, removed);
        let Some(&id) = self.index.get(key) else {
            return false;
        };
        if self.is_expired(id, now) {
            self.remove_node(id, RemovalCause::Expired, removed);
            return false;
        }
        true
    }

    pub(crate) fn insert(
        &mut self,
        key: K,
        value: V,
        hash: u64,
        weight: u32,
        now: u64,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) {
        self.expire_entries(now, removed);
        if let Some(&id) = self.index.get(&key) {
            let (old_deadline, new_deadlines) = {
                let node = self.nodes[id as usize].as_ref().expect("live node");
                let old_deadline = node.expire_at;
                let new_deadlines = self.expiration.on_write(
                    &key,
                    &value,
                    now,
                    Deadlines {
                        at: node.expire_at,
                        write_at: node.write_expire_at,
                    },
                );
                (old_deadline, new_deadlines)
            };
            let (old_value, old_weight, region) = {
                let node = self.nodes[id as usize].as_mut().expect("live node");
                let old_weight = node.weight;
                let region = node.region;
                let old_value = std::mem::replace(&mut node.value, value);
                node.hash = hash;
                node.weight = weight;
                node.write_time = now;
                node.access_time = now;
                node.expire_at = new_deadlines.at;
                node.write_expire_at = new_deadlines.write_at;
                node.refreshing = false;
                (old_value, old_weight, region)
            };
            self.reschedule(id, old_deadline, new_deadlines.at);
            self.sub_region_weight(region, old_weight as u64);
            self.add_region_weight(region, weight as u64);
            self.total_weight = self
                .total_weight
                .saturating_sub(old_weight as u64)
                .saturating_add(weight as u64);
            if weight as u64 > self.global_max_weight {
                // The updated entry no longer fits: Caffeine evicts it with
                // SIZE rather than reporting a replacement.
                drop(old_value);
                self.remove_node(id, RemovalCause::Size, removed);
                self.evict(removed);
                self.climb();
                return;
            }
            if (region == Region::Window) && (weight as u64 > self.window_max) {
                // Between the window and cache maxima the entry is parked at
                // the window's LRU so the next transfer moves it to main.
                // Caffeine reorders without recording a sketch usage here.
                self.move_lru(Region::Window, id);
            } else {
                self.sketch.increment(hash);
                self.touch(region, id);
            }
            removed.push(RemovedEntry {
                key,
                value: old_value,
                cause: RemovalCause::Replaced,
                weight: old_weight,
            });
            self.evict(removed);
            self.climb();
            return;
        }

        let deadlines = self.expiration.on_create(&key, &value, now);
        let id = self.alloc(Node {
            key: key.clone(),
            value,
            hash,
            weight,
            region: Region::Window,
            write_time: now,
            access_time: now,
            expire_at: deadlines.at,
            write_expire_at: deadlines.write_at,
            refreshing: false,
            prev: NIL,
            next: NIL,
        });
        self.index.insert(key, id);
        self.schedule(id, deadlines.at);
        self.window_weight = self.window_weight.saturating_add(weight as u64);
        self.total_weight = self.total_weight.saturating_add(weight as u64);
        if weight as u64 > self.window_max {
            self.link_lru(Region::Window, id);
        } else {
            self.link_mru(Region::Window, id);
        }
        self.sketch.increment(hash);
        // A new entry is a miss in the adaptive-window sample, matching the
        // write that filled it; a plain lookup miss is not counted.
        if self.bounded {
            self.climber.record_miss();
        }
        self.evict(removed);
        self.climb();
    }

    /// Moves a fresh load into the entry captured by a refresh, unless the
    /// entry was removed or replaced in the meantime.
    pub(crate) fn finish_refresh(
        &mut self,
        key: &K,
        captured_write: u64,
        value: V,
        weight: u32,
        now: u64,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) {
        self.expire_entries(now, removed);
        let Some(&id) = self.index.get(key) else {
            return;
        };
        let matches = self.nodes[id as usize]
            .as_ref()
            .map(|node| node.refreshing && node.write_time == captured_write)
            .unwrap_or(false);
        if !matches {
            return;
        }
        let (old_deadline, new_deadlines) = {
            let node = self.nodes[id as usize].as_ref().expect("live node");
            let old_deadline = node.expire_at;
            let new_deadlines = self.expiration.on_write(
                key,
                &value,
                now,
                Deadlines {
                    at: node.expire_at,
                    write_at: node.write_expire_at,
                },
            );
            (old_deadline, new_deadlines)
        };
        let (old_weight, region) = {
            let node = self.nodes[id as usize].as_mut().expect("live node");
            let old_weight = node.weight;
            let region = node.region;
            node.value = value;
            node.weight = weight;
            node.write_time = now;
            node.access_time = now;
            node.expire_at = new_deadlines.at;
            node.write_expire_at = new_deadlines.write_at;
            node.refreshing = false;
            (old_weight, region)
        };
        self.reschedule(id, old_deadline, new_deadlines.at);
        self.sub_region_weight(region, old_weight as u64);
        self.add_region_weight(region, weight as u64);
        self.total_weight = self
            .total_weight
            .saturating_sub(old_weight as u64)
            .saturating_add(weight as u64);
        let hash = self.nodes[id as usize].as_ref().expect("live node").hash;
        // A reload records a climber miss, but not a hit or a sketch usage.
        if self.bounded {
            self.climber.record_miss();
        }
        if weight as u64 > self.global_max_weight {
            self.remove_node(id, RemovalCause::Size, removed);
        } else if (region == Region::Window) && (weight as u64 > self.window_max) {
            self.move_lru(Region::Window, id);
        } else {
            self.sketch.increment(hash);
            self.touch(region, id);
        }
        self.evict(removed);
        self.climb();
    }

    pub(crate) fn clear_refreshing(&mut self, key: &K, captured_write: u64) {
        if let Some(&id) = self.index.get(key) {
            if let Some(node) = self.nodes[id as usize].as_mut() {
                if node.refreshing && node.write_time == captured_write {
                    node.refreshing = false;
                }
            }
        }
    }

    pub(crate) fn start_refresh<Q>(
        &mut self,
        key: &Q,
        now: u64,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) -> Option<(K, u64)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.expire_entries(now, removed);
        let id = *self.index.get(key)?;
        if self.is_expired(id, now) {
            self.remove_node(id, RemovalCause::Expired, removed);
            return None;
        }
        let node = self.nodes[id as usize].as_mut().expect("live node");
        if node.refreshing {
            return None;
        }
        node.refreshing = true;
        Some((node.key.clone(), node.write_time))
    }

    pub(crate) fn remove<Q>(
        &mut self,
        key: &Q,
        cause: RemovalCause,
        now: u64,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.expire_entries(now, removed);
        let id = *self.index.get(key)?;
        let before = removed.len();
        self.remove_node(id, cause, removed);
        removed.get(before).map(|entry| entry.value.clone())
    }

    pub(crate) fn clean_expired(&mut self, now: u64, removed: &mut Vec<RemovedEntry<K, V>>) {
        self.expire_entries(now, removed);
    }

    /// Clones every live entry after removing expired entries.
    pub(crate) fn snapshot(
        &mut self,
        now: u64,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) -> Vec<(K, V)> {
        self.expire_entries(now, removed);
        self.index
            .values()
            .filter_map(|&id| self.nodes[id as usize].as_ref())
            .map(|node| (node.key.clone(), node.value.clone()))
            .collect()
    }

    /// Removes every entry, returning them with an explicit-removal cause.
    pub(crate) fn drain(&mut self) -> Vec<RemovedEntry<K, V>> {
        let mut out = Vec::with_capacity(self.index.len());
        let index = std::mem::take(&mut self.index);
        for id in index.into_values() {
            if let Some(boxed) = self.nodes[id as usize].take() {
                let node = *boxed;
                out.push(RemovedEntry {
                    key: node.key,
                    value: node.value,
                    cause: RemovalCause::Explicit,
                    weight: node.weight,
                });
            }
        }
        self.nodes.clear();
        self.free.clear();
        self.expiry_index.clear();
        self.window = LruList::default();
        self.probation = LruList::default();
        self.protected = LruList::default();
        self.total_weight = 0;
        self.window_weight = 0;
        self.probation_weight = 0;
        self.protected_weight = 0;
        out
    }

    // ------------------------------------------------------------------
    // Expiration
    // ------------------------------------------------------------------

    fn schedule(&mut self, id: u32, deadline: u64) {
        if deadline != NEVER {
            self.expiry_index.insert((deadline, id), ());
        }
    }

    fn unschedule(&mut self, id: u32, deadline: u64) {
        if deadline != NEVER {
            self.expiry_index.remove(&(deadline, id));
        }
    }

    fn reschedule(&mut self, id: u32, old_deadline: u64, new_deadline: u64) {
        if old_deadline != new_deadline {
            self.unschedule(id, old_deadline);
            self.schedule(id, new_deadline);
            if let Some(node) = self.nodes[id as usize].as_mut() {
                node.expire_at = new_deadline;
            }
        }
    }

    fn expire_entries(&mut self, now: u64, removed: &mut Vec<RemovedEntry<K, V>>) {
        if self.expiry_index.is_empty() {
            return;
        }
        while let Some((&(deadline, id), _)) = self.expiry_index.first_key_value() {
            if deadline > now {
                break;
            }
            self.expiry_index.remove(&(deadline, id));
            let expected = self
                .nodes
                .get(id as usize)
                .and_then(|node| node.as_ref())
                .map(|node| node.expire_at);
            if expected == Some(deadline) {
                self.remove_node(id, RemovalCause::Expired, removed);
            }
        }
    }

    fn is_expired(&self, id: u32, now: u64) -> bool {
        let Some(node) = self.nodes[id as usize].as_ref() else {
            return true;
        };
        node.expire_at != NEVER && node.expire_at <= now
    }

    // ------------------------------------------------------------------
    // Adaptive window
    // ------------------------------------------------------------------

    fn record_hit(&mut self, in_window: bool, in_probation: bool) {
        if self.bounded {
            self.climber.record_hit(in_window, in_probation);
        }
    }

    /// Samples the hit rate and resizes the admission window when due.
    fn climb(&mut self) {
        if !self.bounded || self.capacity == 0 {
            return;
        }
        let sample_size = self.sketch.sample_size();
        if self.sketch.is_not_initialized() {
            self.climber.reset_sample();
        } else if self.total_weight < self.capacity / 2 {
            self.climber.discard_sample(self.capacity, sample_size);
        } else {
            self.climber.determine_adjustment(
                self.capacity,
                self.window_max,
                self.protected_max,
                sample_size,
            );
        }
        self.demote_protected_to_limit();
        let adjustment = self.climber.adjustment();
        if adjustment > 0 {
            self.increase_window(adjustment as u64);
        } else if adjustment < 0 {
            self.decrease_window(adjustment.unsigned_abs());
        }
    }

    fn increase_window(&mut self, adjustment: u64) {
        if self.protected_max == 0 {
            return;
        }
        let quota = adjustment.min(self.protected_max);
        self.protected_max -= quota;
        self.window_max = self.window_max.saturating_add(quota);
        self.demote_protected_to_limit();

        let mut remaining = quota;
        for _ in 0..QUEUE_TRANSFER_THRESHOLD {
            let mut source = Region::Probation;
            let mut candidate = self.probation.tail;
            if candidate == NIL || self.node_weight(candidate) > remaining {
                source = Region::Protected;
                candidate = self.protected.tail;
            }
            if candidate == NIL || self.node_weight(candidate) > remaining {
                break;
            }
            remaining -= self.node_weight(candidate);
            self.move_to(candidate, source, Region::Window);
        }

        if remaining > 0 {
            self.protected_max = self.protected_max.saturating_add(remaining);
            self.window_max = self.window_max.saturating_sub(remaining);
        }
        self.climber.carry_over(remaining as i64);
    }

    fn decrease_window(&mut self, adjustment: u64) {
        if self.window_max <= 1 {
            return;
        }
        let quota = adjustment.min(self.window_max - 1);
        self.protected_max = self.protected_max.saturating_add(quota);
        self.window_max -= quota;

        let mut remaining = quota;
        let mut candidate = self.window.tail;
        let mut transfers = 0;
        while candidate != NIL && transfers < QUEUE_TRANSFER_THRESHOLD {
            let next = self.nodes[candidate as usize]
                .as_ref()
                .expect("live node")
                .next;
            let weight = self.node_weight(candidate);
            if weight == 0 {
                candidate = next;
                continue;
            }
            if weight > remaining {
                break;
            }
            remaining -= weight;
            self.move_to(candidate, Region::Window, Region::Probation);
            transfers += 1;
            candidate = next;
        }

        if remaining > 0 {
            self.protected_max = self.protected_max.saturating_sub(remaining);
            self.window_max = self.window_max.saturating_add(remaining);
        }
        self.climber.carry_over(-(remaining as i64));
    }

    fn demote_protected_to_limit(&mut self) {
        let mut transferred = 0;
        while self.protected_weight > self.protected_max
            && self.protected.tail != NIL
            && transferred < QUEUE_TRANSFER_THRESHOLD
        {
            self.demote_protected_lru();
            transferred += 1;
        }
    }

    fn node_weight(&self, id: u32) -> u64 {
        self.nodes[id as usize].as_ref().expect("live node").weight as u64
    }

    // ------------------------------------------------------------------
    // Policy internals
    // ------------------------------------------------------------------

    fn evict(&mut self, removed: &mut Vec<RemovedEntry<K, V>>) {
        let candidate = self.evict_from_window();
        self.evict_from_main(candidate, removed);
    }

    /// Transfers the window's overflow into probation, skipping zero-weight
    /// entries and bounding the work per maintenance cycle. Returns the first
    /// promoted entry, which becomes the first admission candidate.
    fn evict_from_window(&mut self) -> Option<u32> {
        let mut first = None;
        let mut remaining = QUEUE_TRANSFER_THRESHOLD;
        let mut node = self.option(self.window.tail);
        while self.window_weight > self.window_max {
            let Some(id) = node else {
                break;
            };
            if remaining == 0 {
                break;
            }
            let next = self.nodes[id as usize].as_ref().expect("live node").prev;
            if self.node_weight(id) != 0 {
                self.move_to(id, Region::Window, Region::Probation);
                if first.is_none() {
                    first = Some(id);
                }
                remaining -= 1;
            }
            node = self.option(next);
        }
        first
    }

    /// Evicts entries while the cache exceeds its maximum, comparing the
    /// window's admission candidates against the main space's victims by
    /// frequency. Ties prefer the resident victim, matching Caffeine.
    fn evict_from_main(
        &mut self,
        mut candidate: Option<u32>,
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) {
        let mut victim_queue = Region::Probation;
        let mut candidate_queue = Region::Probation;
        let mut victim = self.option(self.probation.tail);

        while self.total_weight > self.capacity {
            if candidate.is_none() && candidate_queue == Region::Probation {
                candidate = self.option(self.window.tail);
                candidate_queue = Region::Window;
            }

            if candidate.is_none() && victim.is_none() {
                match victim_queue {
                    Region::Probation => {
                        victim = self.option(self.protected.tail);
                        victim_queue = Region::Protected;
                        continue;
                    }
                    Region::Protected => {
                        victim = self.option(self.window.tail);
                        victim_queue = Region::Window;
                        continue;
                    }
                    Region::Window => break,
                }
            }

            if let Some(id) = victim {
                if self.node_weight(id) == 0 {
                    victim = self.next_toward_mru(id);
                    continue;
                }
            }
            if let Some(id) = candidate {
                if self.node_weight(id) == 0 {
                    candidate = self.next_toward_mru(id);
                    continue;
                }
            }

            match (victim, candidate) {
                (None, Some(id)) => {
                    candidate = self.evict_and_advance(id, removed);
                }
                (Some(id), None) => {
                    victim = self.evict_and_advance(id, removed);
                }
                (Some(v), Some(c)) if v == c => {
                    victim = self.evict_and_advance(v, removed);
                    candidate = None;
                }
                (Some(v), Some(c)) => {
                    if self.node_weight(c) > self.capacity {
                        candidate = self.evict_and_advance(c, removed);
                        continue;
                    }
                    let candidate_frequency = self.frequency(c);
                    let victim_frequency = self.frequency(v);
                    if admit(candidate_frequency, victim_frequency) {
                        victim = self.evict_and_advance(v, removed);
                        candidate = self.next_toward_mru(c);
                    } else {
                        candidate = self.evict_and_advance(c, removed);
                    }
                }
                (None, None) => break,
            }
        }
    }

    fn evict_and_advance(&mut self, id: u32, removed: &mut Vec<RemovedEntry<K, V>>) -> Option<u32> {
        let next = self.next_toward_mru(id);
        self.remove_node(id, RemovalCause::Size, removed);
        next
    }

    fn next_toward_mru(&self, id: u32) -> Option<u32> {
        let prev = self.nodes[id as usize].as_ref().expect("live node").prev;
        self.option(prev)
    }

    fn frequency(&self, id: u32) -> u8 {
        self.sketch
            .frequency(self.nodes[id as usize].as_ref().expect("live node").hash)
    }

    fn option(&self, id: u32) -> Option<u32> {
        (id != NIL).then_some(id)
    }

    /// Moves the entry between regions, preserving its weight accounting.
    fn move_to(&mut self, id: u32, from: Region, to: Region) {
        let weight = self.node_weight(id);
        self.unlink(from, id);
        self.sub_region_weight(from, weight);
        self.add_region_weight(to, weight);
        self.nodes[id as usize].as_mut().expect("live node").region = to;
        self.link_mru(to, id);
    }

    /// A probation hit earns promotion to the protected segment, unless the
    /// entry is too heavy for it; protected overage is demoted during the next
    /// climber pass, matching Caffeine's deferred adaptation.
    fn touch(&mut self, region: Region, id: u32) {
        match region {
            Region::Window | Region::Protected => self.move_mru(region, id),
            Region::Probation => {
                if self.node_weight(id) > self.protected_max {
                    self.move_mru(Region::Probation, id);
                } else {
                    self.move_to(id, Region::Probation, Region::Protected);
                }
            }
        }
    }

    fn demote_protected_lru(&mut self) {
        let id = self.protected.tail;
        if id != NIL {
            self.move_to(id, Region::Protected, Region::Probation);
        }
    }

    fn move_mru(&mut self, region: Region, id: u32) {
        if self.list(region).head == id {
            return;
        }
        self.unlink(region, id);
        self.link_mru(region, id);
    }

    fn move_lru(&mut self, region: Region, id: u32) {
        if self.list(region).tail == id {
            return;
        }
        self.unlink(region, id);
        self.link_lru(region, id);
    }

    fn alloc(&mut self, node: Node<K, V>) -> u32 {
        if let Some(id) = self.free.pop() {
            self.nodes[id as usize] = Some(Box::new(node));
            id
        } else {
            let id = self.nodes.len() as u32;
            self.nodes.push(Some(Box::new(node)));
            id
        }
    }

    fn remove_node(&mut self, id: u32, cause: RemovalCause, removed: &mut Vec<RemovedEntry<K, V>>) {
        let Some(boxed) = self.nodes[id as usize].take() else {
            return;
        };
        let node = *boxed;
        self.unschedule(id, node.expire_at);
        self.unlink_raw(node.region, id, node.prev, node.next);
        self.sub_region_weight(node.region, node.weight as u64);
        self.total_weight = self.total_weight.saturating_sub(node.weight as u64);
        self.index.remove(&node.key);
        self.free.push(id);
        removed.push(RemovedEntry {
            key: node.key,
            value: node.value,
            cause,
            weight: node.weight,
        });
    }

    fn sub_region_weight(&mut self, region: Region, weight: u64) {
        match region {
            Region::Window => {
                self.window_weight = self.window_weight.saturating_sub(weight);
            }
            Region::Probation => {
                self.probation_weight = self.probation_weight.saturating_sub(weight);
            }
            Region::Protected => {
                self.protected_weight = self.protected_weight.saturating_sub(weight);
            }
        }
    }

    fn add_region_weight(&mut self, region: Region, weight: u64) {
        match region {
            Region::Window => {
                self.window_weight = self.window_weight.saturating_add(weight);
            }
            Region::Probation => {
                self.probation_weight = self.probation_weight.saturating_add(weight);
            }
            Region::Protected => {
                self.protected_weight = self.protected_weight.saturating_add(weight);
            }
        }
    }

    // ------------------------------------------------------------------
    // Intrusive LRU list (head = most recently used, tail = least)
    // ------------------------------------------------------------------

    fn list(&self, region: Region) -> LruList {
        match region {
            Region::Window => self.window,
            Region::Probation => self.probation,
            Region::Protected => self.protected,
        }
    }

    fn list_mut(&mut self, region: Region) -> &mut LruList {
        match region {
            Region::Window => &mut self.window,
            Region::Probation => &mut self.probation,
            Region::Protected => &mut self.protected,
        }
    }

    fn link_mru(&mut self, region: Region, id: u32) {
        let head = self.list(region).head;
        {
            let node = self.nodes[id as usize].as_mut().expect("live node");
            node.prev = NIL;
            node.next = head;
        }
        if head == NIL {
            self.list_mut(region).tail = id;
        } else {
            self.nodes[head as usize].as_mut().expect("live node").prev = id;
        }
        self.list_mut(region).head = id;
    }

    fn link_lru(&mut self, region: Region, id: u32) {
        let tail = self.list(region).tail;
        {
            let node = self.nodes[id as usize].as_mut().expect("live node");
            node.prev = tail;
            node.next = NIL;
        }
        if tail == NIL {
            self.list_mut(region).head = id;
        } else {
            self.nodes[tail as usize].as_mut().expect("live node").next = id;
        }
        self.list_mut(region).tail = id;
    }

    fn unlink(&mut self, region: Region, id: u32) {
        let (prev, next) = {
            let node = self.nodes[id as usize].as_ref().expect("live node");
            (node.prev, node.next)
        };
        self.unlink_raw(region, id, prev, next);
    }

    fn unlink_raw(&mut self, region: Region, id: u32, prev: u32, next: u32) {
        if prev == NIL {
            self.list_mut(region).head = next;
        } else {
            self.nodes[prev as usize].as_mut().expect("live node").next = next;
        }
        if next == NIL {
            self.list_mut(region).tail = prev;
        } else {
            self.nodes[next as usize].as_mut().expect("live node").prev = prev;
        }
        if let Some(node) = self.nodes[id as usize].as_mut() {
            node.prev = NIL;
            node.next = NIL;
        }
    }
}
