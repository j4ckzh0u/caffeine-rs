//! A single cache shard: hash index + intrusive LRU lists + W-TinyLFU policy.
//!
//! Each shard is protected by its own mutex, so unrelated keys do not contend.
//! A shard owns a slice of the cache's total capacity.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use crate::frequency::FrequencySketch;
use crate::removal::{RemovalCause, RemovedEntry};

pub(crate) const NIL: u32 = u32::MAX;

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
    access_time: u64,
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
    sketch: FrequencySketch,
    capacity: u64,
    global_max_weight: u64,
    window_max: u64,
    protected_max: u64,
    total_weight: u64,
    window_weight: u64,
    probation_weight: u64,
    protected_weight: u64,
    expire_after_write: Option<u64>,
    expire_after_access: Option<u64>,
}

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub(crate) fn new(
        capacity: u64,
        global_max_weight: u64,
        initial_capacity: usize,
        expire_after_write: Option<u64>,
        expire_after_access: Option<u64>,
    ) -> Self {
        let window_max = (capacity / 100).max(1).min(capacity);
        let main_capacity = capacity.saturating_sub(window_max);
        let protected_max = ((main_capacity as u128 * 4) / 5) as u64;
        Self {
            index: HashMap::with_capacity(initial_capacity),
            nodes: Vec::with_capacity(initial_capacity),
            free: Vec::new(),
            window: LruList::default(),
            probation: LruList::default(),
            protected: LruList::default(),
            sketch: FrequencySketch::new(capacity),
            capacity,
            global_max_weight,
            window_max,
            protected_max,
            total_weight: 0,
            window_weight: 0,
            probation_weight: 0,
            protected_weight: 0,
            expire_after_write,
            expire_after_access,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.index.len()
    }

    pub(crate) fn weighted_size(&self) -> u64 {
        self.total_weight
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
        let id = *self.index.get(key)?;
        if self.is_expired(id, now) {
            self.remove_node(id, RemovalCause::Expired, removed);
            return None;
        }
        let (value, hash, region, refresh) = {
            let node = self.nodes[id as usize].as_mut().expect("live node");
            node.access_time = now;
            let refresh = match refresh_ttl {
                Some(ttl) if !node.refreshing && now.saturating_sub(node.write_time) >= ttl => {
                    node.refreshing = true;
                    Some((node.key.clone(), node.write_time))
                }
                _ => None,
            };
            (node.value.clone(), node.hash, node.region, refresh)
        };
        self.sketch.increment(hash);
        self.touch(region, id);
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
        if let Some(&id) = self.index.get(&key) {
            if weight as u64 > self.global_max_weight {
                self.remove_node(id, RemovalCause::Replaced, removed);
                return;
            }
            let (old_value, old_weight, region) = {
                let node = self.nodes[id as usize].as_mut().expect("live node");
                let old_weight = node.weight;
                let region = node.region;
                let old_value = std::mem::replace(&mut node.value, value);
                node.hash = hash;
                node.weight = weight;
                node.write_time = now;
                node.access_time = now;
                node.refreshing = false;
                (old_value, old_weight, region)
            };
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
            return;
        }

        if weight as u64 > self.global_max_weight {
            return;
        }

        let id = self.alloc(Node {
            key: key.clone(),
            value,
            hash,
            weight,
            region: Region::Window,
            write_time: now,
            access_time: now,
            refreshing: false,
            prev: NIL,
            next: NIL,
        });
        self.index.insert(key, id);
        self.window_weight = self.window_weight.saturating_add(weight as u64);
        self.total_weight = self.total_weight.saturating_add(weight as u64);
        self.link_mru(Region::Window, id);
        self.sketch.increment(hash);
        self.evict(removed);
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
        let (old_weight, region) = {
            let node = self.nodes[id as usize].as_mut().expect("live node");
            let old_weight = node.weight;
            let region = node.region;
            node.value = value;
            node.weight = weight;
            node.write_time = now;
            node.refreshing = false;
            (old_weight, region)
        };
        self.sub_region_weight(region, old_weight as u64);
        self.add_region_weight(region, weight as u64);
        self.total_weight = self
            .total_weight
            .saturating_sub(old_weight as u64)
            .saturating_add(weight as u64);
        self.evict(removed);
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
        removed: &mut Vec<RemovedEntry<K, V>>,
    ) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let id = *self.index.get(key)?;
        let before = removed.len();
        self.remove_node(id, cause, removed);
        removed.get(before).map(|entry| entry.value.clone())
    }

    pub(crate) fn clean_expired(&mut self, now: u64, removed: &mut Vec<RemovedEntry<K, V>>) {
        let ids: Vec<u32> = self.index.values().copied().collect();
        for id in ids {
            if self.is_expired(id, now) {
                self.remove_node(id, RemovalCause::Expired, removed);
            }
        }
    }

    /// Clones every live entry. Expired entries are skipped, not removed.
    pub(crate) fn snapshot(&self, now: u64) -> Vec<(K, V)> {
        self.index
            .values()
            .filter_map(|&id| self.nodes[id as usize].as_ref())
            .filter(|node| {
                !Self::node_expired(node, now, self.expire_after_write, self.expire_after_access)
            })
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
        let candidate_weight = self.nodes[candidate as usize]
            .as_ref()
            .expect("live node")
            .weight as u64;
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
        let weight = self.nodes[id as usize].as_ref().expect("live node").weight as u64;
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

    fn is_expired(&self, id: u32, now: u64) -> bool {
        let Some(node) = self.nodes[id as usize].as_ref() else {
            return true;
        };
        Self::node_expired(node, now, self.expire_after_write, self.expire_after_access)
    }

    fn node_expired(
        node: &Node<K, V>,
        now: u64,
        expire_after_write: Option<u64>,
        expire_after_access: Option<u64>,
    ) -> bool {
        if let Some(ttl) = expire_after_write {
            now.saturating_sub(node.write_time) >= ttl
        } else if let Some(ttl) = expire_after_access {
            now.saturating_sub(node.access_time) >= ttl
        } else {
            false
        }
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
