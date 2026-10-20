//! A single cache shard: hash index + intrusive LRU lists + W-TinyLFU policy.
//!
//! Each shard is protected by its own mutex, so unrelated keys do not contend.
//! A shard owns a slice of the cache's total capacity.

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use crate::climber::WindowClimber;
use crate::expiry::{Expiration, NEVER};
use crate::frequency::FrequencySketch;
use crate::removal::{RemovalCause, RemovedEntry};

pub(crate) const NIL: u32 = u32::MAX;

/// Caffeine bounds the number of entries transferred per maintenance cycle.
const QUEUE_TRANSFER_THRESHOLD: usize = 1_000;

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
    expire_at: u64,
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
        let Some(&id) = self.index.get(key) else {
            self.record_miss();
            return None;
        };
        if self.is_expired(id, now) {
            self.remove_node(id, RemovalCause::Expired, removed);
            self.record_miss();
            return None;
        }
        let (value, hash, region, refresh, old_deadline, new_deadline, weight) = {
            let node = self.nodes[id as usize].as_mut().expect("live node");
            let old_deadline = node.expire_at;
            let new_deadline = self
                .expiration
                .on_read(&node.key, &node.value, now, old_deadline);
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
                new_deadline,
                node.weight,
            )
        };
        if new_deadline != old_deadline {
            self.reschedule(id, old_deadline, new_deadline);
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
            if weight as u64 > self.global_max_weight {
                self.remove_node(id, RemovalCause::Replaced, removed);
                return;
            }
            let old_deadline = self.nodes[id as usize]
                .as_ref()
                .expect("live node")
                .expire_at;
            let new_deadline = self.expiration.on_write(&key, &value, now, old_deadline);
            let (old_value, old_weight, region) = {
                let node = self.nodes[id as usize].as_mut().expect("live node");
                let old_weight = node.weight;
                let region = node.region;
                let old_value = std::mem::replace(&mut node.value, value);
                node.hash = hash;
                node.weight = weight;
                node.write_time = now;
                node.expire_at = new_deadline;
                node.refreshing = false;
                (old_value, old_weight, region)
            };
            self.reschedule(id, old_deadline, new_deadline);
            self.sub_region_weight(region, old_weight as u64);
            self.add_region_weight(region, weight as u64);
            self.total_weight = self
                .total_weight
                .saturating_sub(old_weight as u64)
                .saturating_add(weight as u64);
            self.sketch.increment(hash);
            self.touch(region, id);
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

        if weight as u64 > self.global_max_weight {
            return;
        }

        let expire_at = self.expiration.on_create(&key, &value, now);
        let id = self.alloc(Node {
            key: key.clone(),
            value,
            hash,
            weight,
            region: Region::Window,
            write_time: now,
            expire_at,
            refreshing: false,
            prev: NIL,
            next: NIL,
        });
        self.index.insert(key, id);
        self.schedule(id, expire_at);
        self.window_weight = self.window_weight.saturating_add(weight as u64);
        self.total_weight = self.total_weight.saturating_add(weight as u64);
        self.link_mru(Region::Window, id);
        self.sketch.increment(hash);
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
        if weight as u64 > self.global_max_weight {
            self.remove_node(id, RemovalCause::Size, removed);
            return;
        }
        let old_deadline = self.nodes[id as usize]
            .as_ref()
            .expect("live node")
            .expire_at;
        let new_deadline = self.expiration.on_write(key, &value, now, old_deadline);
        let (old_weight, region) = {
            let node = self.nodes[id as usize].as_mut().expect("live node");
            let old_weight = node.weight;
            let region = node.region;
            node.value = value;
            node.weight = weight;
            node.write_time = now;
            node.expire_at = new_deadline;
            node.refreshing = false;
            (old_weight, region)
        };
        self.reschedule(id, old_deadline, new_deadline);
        self.sub_region_weight(region, old_weight as u64);
        self.add_region_weight(region, weight as u64);
        self.total_weight = self
            .total_weight
            .saturating_sub(old_weight as u64)
            .saturating_add(weight as u64);
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

    fn record_miss(&mut self) {
        if self.bounded {
            self.climber.record_miss();
        }
        self.climb();
    }

    /// Samples the hit rate and resizes the admission window when due.
    fn climb(&mut self) {
        if !self.bounded || self.capacity == 0 {
            return;
        }
        let sample_size = self.sketch.sample_size();
        if self.total_weight < self.capacity / 2 {
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
        loop {
            if self.window_weight > self.window_max && self.window.tail != NIL {
                self.evict_window_candidate(removed);
            } else if self.protected_weight > self.protected_max {
                self.demote_protected_lru();
            } else if self.total_weight > self.capacity {
                let victim = if self.probation.tail != NIL {
                    self.probation.tail
                } else {
                    self.protected.tail
                };
                if victim == NIL {
                    break;
                }
                self.remove_node(victim, RemovalCause::Size, removed);
            } else {
                break;
            }
        }
    }

    fn evict_window_candidate(&mut self, removed: &mut Vec<RemovedEntry<K, V>>) {
        let candidate = self.window.tail;
        if candidate == NIL {
            return;
        }
        let candidate_weight = self.node_weight(candidate);
        let main_capacity = self.capacity.saturating_sub(self.window_max);
        let main_weight = self.probation_weight.saturating_add(self.protected_weight);

        if main_weight.saturating_add(candidate_weight) <= main_capacity {
            self.move_to(candidate, Region::Window, Region::Probation);
            return;
        }

        if self.probation.tail != NIL {
            let victim = self.probation.tail;
            let candidate_frequency = self.sketch.frequency(
                self.nodes[candidate as usize]
                    .as_ref()
                    .expect("live node")
                    .hash,
            );
            let victim_frequency = self.sketch.frequency(
                self.nodes[victim as usize]
                    .as_ref()
                    .expect("live node")
                    .hash,
            );
            if candidate_frequency > victim_frequency {
                self.remove_node(victim, RemovalCause::Size, removed);
                self.move_to(candidate, Region::Window, Region::Probation);
            } else {
                self.remove_node(candidate, RemovalCause::Size, removed);
            }
        } else if self.protected.tail != NIL {
            // Make room in probation for the admission comparison.
            self.demote_protected_lru();
        } else {
            self.remove_node(candidate, RemovalCause::Size, removed);
        }
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

    /// A probation hit earns promotion to the protected segment.
    fn touch(&mut self, region: Region, id: u32) {
        match region {
            Region::Window | Region::Protected => self.move_mru(region, id),
            Region::Probation => {
                self.move_to(id, Region::Probation, Region::Protected);
                while self.protected_weight > self.protected_max && self.protected.tail != NIL {
                    self.demote_protected_lru();
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
