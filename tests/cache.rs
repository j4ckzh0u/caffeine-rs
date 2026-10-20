//! Behavioral tests for the cache.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use caffeine::{
    Cache, Expiry, ExpiryAccessing, ExpiryCreating, ExpiryWriting, RemovalCause, Ticker,
};

#[derive(Clone, Default)]
struct FakeTicker {
    nanos: Arc<AtomicU64>,
}

impl FakeTicker {
    fn new() -> Self {
        Self::default()
    }

    fn advance(&self, duration: Duration) {
        self.nanos
            .fetch_add(duration.as_nanos() as u64, Ordering::SeqCst);
    }
}

impl Ticker for FakeTicker {
    fn read(&self) -> u64 {
        self.nanos.load(Ordering::SeqCst)
    }
}

type Task = Box<dyn FnOnce() + Send>;

#[derive(Clone, Default)]
struct QueueExecutor {
    tasks: Arc<Mutex<Vec<Task>>>,
}

impl QueueExecutor {
    fn new() -> Self {
        Self::default()
    }

    fn executor(&self) -> impl Fn(Box<dyn FnOnce() + Send>) + Send + Sync + 'static {
        let tasks = Arc::clone(&self.tasks);
        move |task| tasks.lock().unwrap().push(task)
    }

    fn len(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }

    fn run_all(&self) {
        loop {
            let task = self.tasks.lock().unwrap().pop();
            match task {
                Some(task) => task(),
                None => return,
            }
        }
    }
}

type Events = Arc<Mutex<Vec<(u64, u64, RemovalCause)>>>;

fn event_listener(events: &Events) -> impl Fn(&u64, &u64, RemovalCause) + Send + Sync + 'static {
    let events = Arc::clone(events);
    move |key, value, cause| events.lock().unwrap().push((*key, *value, cause))
}

#[test]
fn insert_get_remove() {
    let cache: Cache<String, u32> = Cache::builder().maximum_size(10).build();
    cache.insert("a".to_string(), 1);
    assert_eq!(cache.get("a"), Some(1));
    assert!(cache.contains_key("a"));
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.remove("a"), Some(1));
    assert_eq!(cache.get("a"), None);
    assert!(cache.is_empty());
}

#[test]
fn maximum_size_bounds_entries() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(10).build();
    for key in 0..1_000 {
        cache.insert(key, key);
        assert!(cache.len() <= 10, "len exceeded at {key}");
        assert!(cache.weighted_size() <= 10);
    }
    assert_eq!(cache.get(&999), Some(999));
}

#[test]
fn tinylfu_resists_scans() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(10).build();
    for hot in 0..4 {
        cache.insert(hot, hot);
    }
    for _ in 0..5 {
        for hot in 0..4 {
            assert_eq!(cache.get(&hot), Some(hot));
        }
    }
    // Move the last hot key out of the admission window, then promote every
    // hot key into the protected segment.
    cache.insert(4, 4);
    cache.insert(5, 5);
    for hot in 0..4 {
        assert_eq!(cache.get(&hot), Some(hot));
    }
    // A one-shot scan must not flush the hot working set.
    for scan in 100..200 {
        cache.insert(scan, scan);
    }
    for hot in 0..4 {
        assert_eq!(cache.get(&hot), Some(hot), "hot key {hot} was evicted");
    }
}

#[test]
fn expire_after_write_removes_and_notifies() {
    let ticker = FakeTicker::new();
    let events: Events = Arc::new(Mutex::new(Vec::new()));
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .expire_after_write(Duration::from_secs(5))
        .ticker(ticker.clone())
        .removal_listener(event_listener(&events))
        .build();

    cache.insert(1, 1);
    assert_eq!(cache.get(&1), Some(1));
    ticker.advance(Duration::from_secs(5));
    assert_eq!(cache.get(&1), None);

    let events = events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], (1, 1, RemovalCause::Expired));
}

#[test]
fn expire_after_access_extends_on_read() {
    let ticker = FakeTicker::new();
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .expire_after_access(Duration::from_secs(5))
        .ticker(ticker.clone())
        .build();

    cache.insert(1, 1);
    ticker.advance(Duration::from_secs(3));
    assert_eq!(cache.get(&1), Some(1));
    ticker.advance(Duration::from_secs(3));
    assert_eq!(cache.get(&1), Some(1));
    ticker.advance(Duration::from_secs(5));
    assert_eq!(cache.get(&1), None);
}

#[test]
fn peek_does_not_extend_access_ttl() {
    let ticker = FakeTicker::new();
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .expire_after_access(Duration::from_secs(5))
        .ticker(ticker.clone())
        .build();

    cache.insert(1, 1);
    ticker.advance(Duration::from_secs(3));
    assert_eq!(cache.peek(&1), Some(1));
    ticker.advance(Duration::from_secs(2));
    assert_eq!(cache.get(&1), None);
}

#[test]
fn refresh_returns_stale_then_updates() {
    let ticker = FakeTicker::new();
    let queue = QueueExecutor::new();
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .expire_after_write(Duration::from_secs(60))
        .refresh_after_write(Duration::from_secs(5))
        .ticker(ticker.clone())
        .executor(queue.executor())
        .record_stats(true)
        .build_with_loader(|&key| Some(key + 100));

    cache.insert(1, 1);
    ticker.advance(Duration::from_secs(5));

    assert_eq!(cache.get(&1), Some(1), "stale value is returned");
    assert_eq!(queue.len(), 1);

    // A second stale read is coalesced while the refresh is in flight.
    assert_eq!(cache.get(&1), Some(1));
    assert_eq!(queue.len(), 1);

    queue.run_all();
    assert_eq!(cache.get(&1), Some(101));
    assert_eq!(cache.stats().load_success_count, 1);
}

#[test]
fn failed_refresh_keeps_old_value_and_can_retry() {
    let ticker = FakeTicker::new();
    let queue = QueueExecutor::new();
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .refresh_after_write(Duration::from_secs(5))
        .ticker(ticker.clone())
        .executor(queue.executor())
        .record_stats(true)
        .build_with_loader(|_| None);

    cache.insert(1, 1);
    ticker.advance(Duration::from_secs(5));
    assert_eq!(cache.get(&1), Some(1));
    queue.run_all();
    assert_eq!(cache.get(&1), Some(1));
    assert_eq!(cache.stats().load_failure_count, 1);

    // The in-flight guard was cleared, so a later stale read retries.
    assert_eq!(queue.len(), 1);
}

#[test]
fn explicit_refresh_schedules_loader() {
    let queue = QueueExecutor::new();
    let ticker = FakeTicker::new();
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .ticker(ticker)
        .executor(queue.executor())
        .build_with_loader(|&key| Some(key * 2));

    cache.insert(3, 3);
    assert!(cache.refresh(&3));
    assert!(!cache.refresh(&3), "single-flight");
    queue.run_all();
    assert_eq!(cache.get(&3), Some(6));
}

#[test]
fn replacement_and_explicit_removals_are_notified() {
    let events: Events = Arc::new(Mutex::new(Vec::new()));
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .removal_listener(event_listener(&events))
        .build();

    cache.insert(1, 10);
    cache.insert(1, 20);
    cache.invalidate(&1);

    let events = events.lock().unwrap();
    assert_eq!(
        *events,
        vec![
            (1, 10, RemovalCause::Replaced),
            (1, 20, RemovalCause::Explicit),
        ]
    );
}

#[test]
fn size_eviction_notifies_eviction_listener() {
    let evictions: Events = Arc::new(Mutex::new(Vec::new()));
    let removals: Events = Arc::new(Mutex::new(Vec::new()));
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(2)
        .eviction_listener(event_listener(&evictions))
        .removal_listener(event_listener(&removals))
        .build();

    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.insert(3, 3);

    assert!(cache.len() <= 2);
    let evictions = evictions.lock().unwrap();
    assert!(!evictions.is_empty());
    assert!(evictions.iter().all(|event| event.2 == RemovalCause::Size));
    let removals = removals.lock().unwrap();
    assert_eq!(removals.len(), evictions.len());
}

#[test]
fn statistics_are_recorded_only_when_enabled() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(10).record_stats(true).build();
    cache.insert(1, 1);
    assert_eq!(cache.get(&1), Some(1));
    assert_eq!(cache.get(&2), None);
    assert_eq!(cache.get_with(2, |_| 2), 2);
    let failed: Result<u64, &str> = cache.try_get_with(3, |_| Err("nope"));
    assert!(failed.is_err());

    let stats = cache.stats();
    assert_eq!(stats.hit_count, 1);
    assert_eq!(stats.miss_count, 3);
    assert_eq!(stats.load_success_count, 1);
    assert_eq!(stats.load_failure_count, 1);
    assert_eq!(stats.request_count(), 4);
    assert_eq!(stats.load_count(), 2);
    assert!(stats.hit_rate() > 0.0 && stats.hit_rate() < 1.0);

    let quiet: Cache<u64, u64> = Cache::builder().maximum_size(10).build();
    quiet.insert(1, 1);
    quiet.get(&1);
    assert_eq!(quiet.stats(), caffeine::CacheStats::default());
}

#[test]
fn eviction_count_tracks_policy_removals() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(3).record_stats(true).build();
    for key in 0..100 {
        cache.insert(key, key);
    }
    let stats = cache.stats();
    assert_eq!(stats.eviction_count, 97);
    assert_eq!(stats.evicted_weight, 97);
    assert_eq!(cache.len(), 3);
}

#[test]
fn weighted_eviction_and_oversized_entries() {
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_weight(10)
        .weigher(|_, value| *value as u32)
        .build();

    cache.insert(1, 6);
    cache.insert(2, 6);
    assert_eq!(cache.len(), 1);
    assert!(cache.weighted_size() <= 10);

    cache.insert(3, 100);
    assert!(!cache.contains_key(&3));
    assert!(cache.policy().is_weighted());
    assert_eq!(cache.policy().maximum(), 10);
}

#[test]
fn get_with_caches_loaded_value() {
    let loads = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&loads);
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(10).build();

    let value = cache.get_with(7, |&key| {
        counter.fetch_add(1, Ordering::SeqCst);
        key * 3
    });
    assert_eq!(value, 21);
    assert_eq!(cache.get(&7), Some(21));
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    cache.get_with(7, |&key| {
        counter.fetch_add(1, Ordering::SeqCst);
        key * 4
    });
    assert_eq!(loads.load(Ordering::SeqCst), 1, "cached value was reused");
}

#[test]
fn retain_iter_and_clear() {
    let events: Events = Arc::new(Mutex::new(Vec::new()));
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(100)
        .removal_listener(event_listener(&events))
        .build();
    for key in 0..10 {
        cache.insert(key, key * 10);
    }

    cache.retain(|key, _| key % 2 == 0);
    assert_eq!(cache.len(), 5);
    let mut items: Vec<(u64, u64)> = cache.iter().collect();
    items.sort_unstable();
    assert_eq!(items, vec![(0, 0), (2, 20), (4, 40), (6, 60), (8, 80)]);

    cache.clear();
    assert!(cache.is_empty());
    assert_eq!(events.lock().unwrap().len(), 10);
}

#[test]
fn clean_up_removes_expired_entries_without_access() {
    let ticker = FakeTicker::new();
    let events: Events = Arc::new(Mutex::new(Vec::new()));
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .expire_after_write(Duration::from_secs(1))
        .ticker(ticker.clone())
        .removal_listener(event_listener(&events))
        .build();

    cache.insert(1, 1);
    ticker.advance(Duration::from_secs(1));
    assert_eq!(cache.len(), 1, "expiration is lazy");
    cache.clean_up();
    assert!(cache.is_empty());
    assert_eq!(events.lock().unwrap()[0].2, RemovalCause::Expired);
}

#[test]
fn unbounded_cache_retains_everything() {
    let cache: Cache<u64, u64> = Cache::builder().build();
    for key in 0..1_000 {
        cache.insert(key, key);
    }
    assert_eq!(cache.len(), 1_000);
    assert_eq!(cache.get(&0), Some(0));
}

#[test]
fn policy_exposes_configuration() {
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(42)
        .expire_after_access(Duration::from_secs(2))
        .build();
    let policy = cache.policy();
    assert_eq!(policy.maximum(), 42);
    assert!(!policy.is_weighted());
    assert_eq!(policy.expire_after_access(), Some(Duration::from_secs(2)));
    assert_eq!(policy.expire_after_write(), None);
    assert_eq!(policy.estimated_size(), 0);
}

#[test]
fn concurrent_access_is_safe_and_bounded() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(256).build();
    let mut handles = Vec::new();
    for thread_id in 0..8u64 {
        let cache = cache.clone();
        handles.push(thread::spawn(move || {
            for i in 0..2_000u64 {
                let key = (i * 7 + thread_id) % 1_000;
                cache.insert(key, i);
                cache.get(&key);
                if i % 100 == 0 {
                    cache.get_with(key + 1, |&k| k * 2);
                }
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert!(cache.len() <= 256);
    assert!(cache.weighted_size() <= 256);
}

#[test]
fn cache_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Cache<String, String>>();
}

#[test]
#[should_panic(expected = "maximum_size and maximum_weight")]
fn conflicting_size_and_weight_panics() {
    let _: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .maximum_weight(10)
        .weigher(|_, _| 1)
        .build();
}

#[test]
#[should_panic(expected = "requires a weigher")]
fn weight_without_weigher_panics() {
    let _: Cache<u64, u64> = Cache::builder().maximum_weight(10).build();
}

#[test]
#[should_panic(expected = "requires a loader")]
fn refresh_without_loader_panics() {
    let _: Cache<u64, u64> = Cache::builder()
        .refresh_after_write(Duration::from_secs(1))
        .build();
}

#[test]
fn variable_expiry_uses_per_entry_deadlines() {
    let ticker = FakeTicker::new();
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(16)
        .ticker(ticker.clone())
        .expire_after(ExpiryCreating::new(|_key: &u64, value: &u64| {
            Some(Duration::from_secs(*value))
        }))
        .build();

    cache.insert(1, 2);
    cache.insert(2, 10);
    assert!(cache.policy().is_variable_expiration());
    assert_eq!(cache.policy().expire_after_write(), None);

    ticker.advance(Duration::from_secs(3));
    assert_eq!(cache.get(&1), None);
    assert_eq!(cache.get(&2), Some(10));

    ticker.advance(Duration::from_secs(8));
    assert_eq!(cache.get(&2), None);
}

#[test]
fn variable_expiry_extends_on_read() {
    let ticker = FakeTicker::new();
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(16)
        .ticker(ticker.clone())
        .expire_after(ExpiryAccessing::new(|_key: &u64, _value: &u64| {
            Some(Duration::from_secs(5))
        }))
        .build();

    cache.insert(1, 1);
    ticker.advance(Duration::from_secs(4));
    assert_eq!(cache.get(&1), Some(1));

    ticker.advance(Duration::from_secs(4));
    assert_eq!(cache.get(&1), Some(1), "read should extend the deadline");

    ticker.advance(Duration::from_secs(6));
    assert_eq!(cache.get(&1), None);
}

#[test]
fn variable_expiry_update_recalculates_deadline() {
    let ticker = FakeTicker::new();
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(16)
        .ticker(ticker.clone())
        .expire_after(ExpiryWriting::new(|_key: &u64, value: &u64| {
            Some(Duration::from_secs(*value))
        }))
        .build();

    cache.insert(1, 10);
    ticker.advance(Duration::from_secs(5));
    cache.insert(1, 2);
    ticker.advance(Duration::from_secs(3));
    assert_eq!(cache.get(&1), None, "update should shorten the lifetime");
}

struct RemainingProbe(Arc<Mutex<Vec<Option<Duration>>>>);

impl Expiry<u64, u64> for RemainingProbe {
    fn expire_after_create(&self, _key: &u64, _value: &u64, _now: u64) -> Option<Duration> {
        Some(Duration::from_secs(10))
    }

    fn expire_after_update(
        &self,
        _key: &u64,
        _value: &u64,
        _now: u64,
        current: Option<Duration>,
    ) -> Option<Duration> {
        self.0.lock().unwrap().push(current);
        current
    }
}

#[test]
fn variable_expiry_passes_remaining_lifetime_to_update() {
    let ticker = FakeTicker::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(16)
        .ticker(ticker.clone())
        .expire_after(RemainingProbe(Arc::clone(&seen)))
        .build();

    cache.insert(1, 1);
    ticker.advance(Duration::from_secs(4));
    cache.insert(1, 2);
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &[Some(Duration::from_secs(6))]
    );
}

#[test]
fn expiration_maintenance_removes_every_due_entry() {
    let ticker = FakeTicker::new();
    let events: Events = Arc::new(Mutex::new(Vec::new()));
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(16)
        .ticker(ticker.clone())
        .expire_after(ExpiryCreating::new(|_key: &u64, _value: &u64| {
            Some(Duration::from_secs(1))
        }))
        .removal_listener(event_listener(&events))
        .build();

    for key in 0..5 {
        cache.insert(key, key);
    }
    ticker.advance(Duration::from_secs(2));
    assert!(!cache.contains_key(&99));
    assert_eq!(cache.len(), 0);
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 5);
    assert!(events
        .iter()
        .all(|(_, _, cause)| *cause == RemovalCause::Expired));
}

#[test]
fn adaptive_window_grows_for_recency_workload() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(10).build();
    assert_eq!(cache.policy().window_maximum(), 1);

    for key in 0..10 {
        cache.insert(key, key);
    }
    for _ in 0..40 {
        for key in 5..10 {
            assert!(cache.get(&key).is_some());
        }
    }

    assert!(
        cache.policy().window_maximum() > 1,
        "hill climber should widen the admission window"
    );
}

#[test]
#[should_panic(expected = "mutually exclusive")]
fn custom_expiry_conflicts_with_fixed_expiry() {
    let _: Cache<u64, u64> = Cache::builder()
        .expire_after_write(Duration::from_secs(1))
        .expire_after(ExpiryAccessing::new(|_key: &u64, _value: &u64| {
            Some(Duration::from_secs(1))
        }))
        .build();
}
