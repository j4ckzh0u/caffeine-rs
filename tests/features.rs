use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use caffeine::guava::GuavaCache;
use caffeine::jcache::{CachingProvider, Configuration};
use caffeine::simulator::{simulate, Policy as SimulatorPolicy};
use caffeine::{AsyncCache, AsyncLoadingCache, Cache, ExpiryCreating, ReferenceCache, Ticker};

struct FakeTicker(Arc<AtomicU64>);

impl Ticker for FakeTicker {
    fn read(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

struct ThreadWaker(thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos() as u64
}

#[test]
fn policy_eviction_view_resizes_and_snapshots() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(4).build();
    for key in 0..6 {
        cache.insert(key, key * 10);
    }
    let policy = cache.policy();
    let eviction = policy.eviction().expect("bounded cache");
    assert_eq!(eviction.maximum(), 4);
    assert!(cache.len() <= 4);
    assert_eq!(eviction.coldest(2).len(), 2);
    assert_eq!(eviction.hottest(2).len(), 2);

    eviction.set_maximum(1);
    assert!(cache.len() <= 1);
    assert_eq!(cache.policy().eviction().unwrap().maximum(), 1);
}

#[test]
fn policy_eviction_view_reports_weights() {
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_weight(10)
        .weigher(|_, value| *value as u32)
        .build();
    cache.insert(1, 4);

    let policy = cache.policy();
    let eviction = policy.eviction().unwrap();
    assert!(eviction.is_weighted());
    assert_eq!(eviction.weight_of(&1), Some(4));
    assert_eq!(eviction.weighted_size(), Some(4));

    let unweighted: Cache<u64, u64> = Cache::builder().maximum_size(2).build();
    assert_eq!(unweighted.policy().eviction().unwrap().weight_of(&1), None);
}

#[test]
fn fixed_expiration_view_reads_and_updates_deadlines() {
    let clock = Arc::new(AtomicU64::new(0));
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .expire_after_access(Duration::from_secs(2))
        .ticker(FakeTicker(Arc::clone(&clock)))
        .build();
    cache.insert(1, 10);

    let policy = cache.policy();
    let expiration = policy.expire_after_access().unwrap();
    assert_eq!(expiration.expires_after(), Duration::from_secs(2));
    assert_eq!(expiration.age_of(&1), Some(Duration::ZERO));

    clock.fetch_add(nanos(Duration::from_secs(1)), Ordering::Relaxed);
    assert_eq!(cache.age_of(&1, true), Some(Duration::from_secs(1)));
    assert_eq!(cache.remaining_duration(&1), Some(Duration::from_secs(1)));

    expiration.set_expires_after(Duration::from_secs(5));
    clock.fetch_add(nanos(Duration::from_secs(3)), Ordering::Relaxed);
    assert_eq!(cache.get(&1), Some(10));
    clock.fetch_add(nanos(Duration::from_secs(2)), Ordering::Relaxed);
    assert_eq!(cache.get(&1), Some(10));
}

#[test]
fn variable_expiration_view_sets_per_entry_deadlines() {
    let clock = Arc::new(AtomicU64::new(0));
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(10)
        .expire_after(ExpiryCreating::new(|_key: &u64, value: &u64| {
            Some(Duration::from_secs(*value))
        }))
        .ticker(FakeTicker(Arc::clone(&clock)))
        .build();
    let policy = cache.policy();
    let expiration = policy.expire_variably().unwrap();
    expiration.put(1, 30, Duration::from_secs(2));
    assert_eq!(
        expiration.get_expires_after(&1),
        Some(Duration::from_secs(2))
    );
    assert_eq!(
        expiration.put_if_absent(1, 99, Duration::from_secs(1)),
        Some(30)
    );

    clock.fetch_add(nanos(Duration::from_secs(3)), Ordering::Relaxed);
    assert_eq!(cache.get(&1), None);
}

#[test]
fn quiet_peek_does_not_record_statistics() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(10).record_stats(true).build();
    cache.insert(1, 10);
    assert_eq!(cache.policy().get_if_present_quietly(&1), Some(10));
    assert_eq!(cache.stats().request_count(), 0);
    assert_eq!(cache.policy().keys(), vec![1]);
}

#[test]
fn weak_values_are_collected_and_counted() {
    let cache: ReferenceCache<u64, String> = ReferenceCache::builder()
        .maximum_size(10)
        .weak_values()
        .record_stats(true)
        .build();
    let value = Arc::new("alive".to_string());
    cache.insert(Arc::new(1), Arc::clone(&value));
    assert_eq!(
        cache.get(&Arc::new(1)).map(|value| (*value).clone()),
        Some("alive".to_string())
    );

    drop(value);
    assert!(cache.get(&Arc::new(1)).is_none());
    assert_eq!(cache.stats().eviction_count, 1);
}

#[test]
fn strong_values_survive_dropping_the_caller_handle() {
    let cache: ReferenceCache<u64, String> = ReferenceCache::builder().maximum_size(10).build();
    let value = Arc::new("kept".to_string());
    cache.insert(Arc::new(1), Arc::clone(&value));
    drop(value);
    assert_eq!(
        cache.get(&Arc::new(1)).map(|value| (*value).clone()),
        Some("kept".to_string())
    );
}

#[test]
fn weak_keys_use_pointer_identity() {
    let cache: ReferenceCache<String, u64> = ReferenceCache::builder()
        .maximum_size(10)
        .weak_keys()
        .build();
    let first = Arc::new("same".to_string());
    let second = Arc::new("same".to_string());
    cache.insert(Arc::clone(&first), Arc::new(1));
    cache.insert(Arc::clone(&second), Arc::new(2));
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.get(&first).map(|value| *value), Some(1));
    assert_eq!(cache.get(&second).map(|value| *value), Some(2));
    assert!(cache.get(&Arc::new("same".to_string())).is_none());
}

#[test]
fn soft_values_are_purged_on_clean_up() {
    let cache: ReferenceCache<u64, String> = ReferenceCache::builder()
        .maximum_size(10)
        .soft_values()
        .build();
    let value = Arc::new("soft".to_string());
    cache.insert(Arc::new(1), Arc::clone(&value));
    drop(value);
    cache.clean_up();
    assert!(cache.get(&Arc::new(1)).is_none());
}

#[test]
fn async_cache_loads_once_and_reuses_the_value() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(10).record_stats(true).build();
    let async_cache = AsyncCache::new(cache);
    let loads = Arc::new(AtomicUsize::new(0));

    let loader = {
        let loads = Arc::clone(&loads);
        move |key: u64| async move {
            loads.fetch_add(1, Ordering::SeqCst);
            key * 10
        }
    };
    assert_eq!(block_on(async_cache.get_with_async(1, loader)), 10);
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    let again = {
        let loads = Arc::clone(&loads);
        move |key: u64| async move {
            loads.fetch_add(1, Ordering::SeqCst);
            key * 10
        }
    };
    assert_eq!(block_on(async_cache.get_with_async(1, again)), 10);
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(block_on(async_cache.get(1)), Some(10));
    assert_eq!(async_cache.stats().load_success_count, 1);
}

#[test]
fn async_try_get_with_propagates_errors() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(10).record_stats(true).build();
    let async_cache = AsyncCache::new(cache);

    let result: Result<u64, &str> =
        block_on(async_cache.try_get_with(1, |_| async { Err("backend down") }));
    assert_eq!(result, Err("backend down"));
    assert_eq!(async_cache.stats().load_failure_count, 1);

    let value = block_on(async_cache.get_with_async(1, |key| async move { key + 1 }));
    assert_eq!(value, 2);
}

#[test]
fn async_loading_cache_runs_its_loader() {
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(10).build();
    let loading = AsyncLoadingCache::new(cache, |key: u64| Box::pin(async move { key * 2 }));
    assert_eq!(block_on(loading.get(3)), 6);
    assert_eq!(block_on(loading.get(3)), 6);
    assert_eq!(loading.synchronous().len(), 1);
}

#[test]
fn simulator_compares_policies() {
    let lru = simulate([1, 2, 1, 3, 1], SimulatorPolicy::Lru, 2);
    assert_eq!(lru.request_count(), 5);
    assert_eq!(lru.hits, 2);
    assert_eq!(lru.evictions, 1);

    let lfu = simulate([1, 2, 1, 3, 1], SimulatorPolicy::Lfu, 2);
    assert_eq!(lfu.request_count(), 5);

    let wtinylfu = simulate([1, 2, 1, 3, 1], SimulatorPolicy::Wtinylfu, 2);
    assert_eq!(wtinylfu.request_count(), 5);
}

#[test]
fn guava_adapter_exposes_guava_vocabulary() {
    let cache: GuavaCache<u64, u64> = GuavaCache::builder().maximum_size(10).build();
    cache.put(1, 10);
    cache.put_all([(2, 20), (3, 30)]);
    assert_eq!(cache.get_if_present(&1), Some(10));
    assert_eq!(cache.get_if_present(&2), Some(20));
    assert_eq!(cache.get_all_present(&[&2, &3]).len(), 2);
    assert_eq!(cache.get_with(1, |key| key * 100), 10);
    assert_eq!(cache.as_map().len(), 3);
}

#[test]
fn jcache_facade_tracks_configuration_and_names() {
    let provider = CachingProvider::new();
    let manager = provider.get_cache_manager();
    let cache: Cache<u64, u64> = Cache::builder().maximum_size(10).build();
    let configuration = Configuration::<u64, u64>::new().set_statistics(true);
    let jcache = manager.create_cache("numbers", configuration, cache);

    jcache.put(1, 10);
    assert_eq!(jcache.get(&1), Some(10));
    assert_eq!(jcache.put_if_absent(1, 99), Some(10));
    assert_eq!(jcache.size(), 1);
    assert!(jcache.configuration().is_statistics_enabled());
    assert_eq!(manager.cache_names(), vec!["numbers".to_string()]);
    assert!(manager.get_cache::<u64, u64>("numbers").is_some());

    jcache.close();
    assert!(jcache.is_closed());
    manager.destroy_cache("numbers");
    assert!(manager.cache_names().is_empty());
}
