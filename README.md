# caffeine-rs

[![CI](https://github.com/j4ckzh0u/caffeine-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/j4ckzh0u/caffeine-rs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A high-performance, near-optimal in-memory cache for Rust, a functional Rust
port of [Ben Manes' Caffeine](https://github.com/ben-manes/caffeine).

The eviction policy is **Window TinyLFU (W-TinyLFU)**: a small admission LRU
window backed by a segmented LRU main space, with a 4-bit Count-Min sketch
estimating how often each key has been used. Scans and one-hit wonders are
rejected instead of flushing the working set.

No dependencies. No `unsafe`.

## Quick start

```rust
use caffeine::Cache;
use std::time::Duration;

let cache: Cache<String, u64> = Cache::builder()
    .maximum_size(10_000)
    .expire_after_write(Duration::from_secs(300))
    .record_stats(true)
    .build();

cache.insert("answer".to_string(), 42);
assert_eq!(cache.get("answer"), Some(42));
```

Run the example:

```bash
cargo run --example basic
```

## Features

| Caffeine feature | caffeine-rs | Notes |
|---|---|---|
| Size-based eviction | ✅ | `maximum_size`, W-TinyLFU |
| Weight-based eviction | ✅ | `maximum_weight` + `weigher` |
| Expire after write | ✅ | lazy expiration + `clean_up` |
| Expire after access | ✅ | idle timeout |
| Both fixed TTLs at once | ✅ | expires when either timeout fires first |
| Variable per-entry expiry (`Expiry`) | ✅ | create, update, and read hooks |
| Hill-climbing adaptive window | ✅ | reactive tier up to 4096 entries |
| Density-tier window heuristics | ✅ | probes, audits, anchors above 4096 entries |
| Refresh after write | ✅ | async, one in-flight refresh per key |
| Automatic loading | ✅ | `get_loaded` / `get_all_loaded` / `get_with` / `try_get_with` |
| Removal listener | ✅ | synchronous, gets the `RemovalCause` |
| Eviction listener | ✅ | size/weight, expiration, and collected removals |
| Statistics | ✅ | hits, misses, loads, load time, evictions, `plus`/`minus` |
| Policy inspection and resizing | ✅ | eviction, expiration, and refresh views; weighted coldest/hottest |
| Map view | ✅ | live `as_map()` with read/write operations |
| Custom ticker | ✅ | deterministic time in tests |
| Custom executor | ✅ | drives refresh tasks |
| Weak / soft references | ✅ | `ReferenceCache`: weak/soft values, identity weak keys |
| Async cache views | ✅ | `AsyncCache` / `AsyncLoadingCache`, builder constructors |
| Guava adapter | ✅ | `guava` module: builder, cache, loading cache, conversions |
| JCache adapter | ✅ | `jcache` module: JSR-107-shaped provider, manager, cache |
| Simulator | ✅ | trace replay for W-TinyLFU, LRU, LFU, FIFO, SLRU, CLOCK, and Optimal |

## Usage

### Manual population and loading

```rust
use caffeine::Cache;
use std::sync::atomic::{AtomicU64, Ordering};

let loads = AtomicU64::new(0);
let cache: Cache<u64, u64> = Cache::builder().maximum_size(1_000).build();

cache.put(7, 70);
cache.get(7);                       // Some(70), records a hit
cache.get(8);                       // None, records a miss

let value = cache.get_with(8, |key| {
    loads.fetch_add(1, Ordering::Relaxed);
    key * 10
});
assert_eq!(value, 80);

let result: Result<u64, &str> = cache.try_get_with(9, |_| Err("backend down"));
assert!(result.is_err());
```

A cache built with `build_with_loader` exposes `get_loaded` and
`get_all_loaded`, matching `LoadingCache.get` / `getAll`:

```rust
let cache: caffeine::Cache<u64, u64> = caffeine::Cache::builder()
    .maximum_size(1_000)
    .build_with_loader(|key| Some(key * 10));

assert_eq!(cache.get_loaded(3), Some(30));
assert_eq!(cache.get_all_loaded([4, 5]).len(), 2);
```

`get_with` coalesces concurrent misses on the same key: one thread runs the
initializer while the others wait and reuse the cached value, matching
Caffeine's per-key computation. If the load fails (or panics), the waiters
wake and retry, so a failed `try_get_with` is not shared with them. Loading the
same key recursively from inside its own initializer panics instead of
deadlocking.

### Weighted entries

```rust
use caffeine::Cache;

let cache: Cache<u64, Vec<u8>> = Cache::builder()
    .maximum_weight(1_000)
    .weigher(|_key, bytes| bytes.len() as u32)
    .build();

cache.insert(1, vec![0; 400]);
cache.insert(2, vec![0; 400]);
cache.insert(3, vec![0; 400]); // one entry is evicted to stay under 1,000
```

An entry heavier than the cache's maximum weight is not cached.

### Expiration

```rust
use caffeine::{Cache, RemovalCause, Ticker};
use std::time::Duration;

// Use your own Ticker in tests instead of sleeping.
let cache: Cache<u64, u64> = Cache::builder()
    .maximum_size(100)
    .expire_after_access(Duration::from_secs(30))
    .removal_listener(|key, _value, cause| {
        if cause == RemovalCause::Expired {
            eprintln!("{key} expired");
        }
    })
    .build();
```

Expiration is maintained on access, insert, and iteration, and by `clean_up`.
Each shard keeps an ordered deadline index, so all due entries are removed in
one pass rather than only the key being touched. `len` can still briefly
include entries whose deadline has passed but which no operation has
maintained yet, matching Caffeine's maintenance model.

`expire_after_write` and `expire_after_access` may be set together. Reads
reset only the idle timeout; the write deadline still stands, and the entry
expires when either deadline arrives first — exactly Caffeine's `hasExpired`
rule.

For per-entry lifetimes, implement `Expiry` or use one of the built-in
helpers:

```rust
use caffeine::{Cache, ExpiryAccessing};
use std::time::Duration;

let cache: Cache<u64, u64> = Cache::builder()
    .maximum_size(100)
    .expire_after(ExpiryAccessing::new(|_key, value| {
        // Each entry gets its own idle timeout, returned in Option<Duration>.
        Some(Duration::from_secs(*value))
    }))
    .build();

cache.insert(1, 30); // 30-second idle timeout
```

The update and read hooks receive the entry's **remaining** lifetime as
`Option<Duration>`; returning it unchanged preserves the deadline. `None`
means the entry never expires. Custom callbacks run while the shard lock is
held, so they must not re-enter the cache.

### Asynchronous refresh

```rust
use caffeine::Cache;
use std::time::Duration;

let cache: Cache<u64, String> = Cache::builder()
    .maximum_size(10_000)
    .expire_after_write(Duration::from_secs(300))
    .refresh_after_write(Duration::from_secs(60))
    .build_with_loader(|key| {
        // Return None to keep the previous value.
        fetch_from_database(*key)
    });

let value = cache.get(&42); // returns the stale value while refreshing
```

The first stale read after the refresh interval returns the old value and
schedules the loader on the cache's executor. Further stale reads are
coalesced until that refresh finishes. A failed refresh keeps the old value
and clears the in-flight guard.

The default executor spawns a named OS thread per refresh. Supply
`executor(...)` to route refreshes onto your own thread pool or runtime.

### Statistics

```rust
let cache: caffeine::Cache<u64, u64> = caffeine::Cache::builder()
    .maximum_size(1_000)
    .record_stats(true)
    .build();

// ... traffic ...

let stats = cache.stats();
println!(
    "hit rate {:.2}, evictions {}, avg load {:?}",
    stats.hit_rate(),
    stats.eviction_count,
    stats.average_load_penalty(),
);
```

Statistics are zero unless `record_stats(true)` is set.

### Policy view

```rust
let cache: caffeine::Cache<u64, u64> = caffeine::Cache::builder()
    .maximum_size(1_000)
    .build();

let policy = cache.policy();
assert_eq!(policy.maximum(), 1_000);
assert!(!policy.is_weighted());
assert_eq!(policy.estimated_size(), 0);
```

### Weak and soft references

`ReferenceCache` mirrors Caffeine's `weakKeys`, `weakValues`, and
`softValues`. Insert and look up entries with `Arc` handles; the cache stores
`Weak` handles internally when the corresponding strength is weak or soft.

```rust
use caffeine::ReferenceCache;
use std::sync::Arc;

let cache: ReferenceCache<u64, String> = ReferenceCache::builder()
    .maximum_size(10_000)
    .weak_values()
    .build();

let value = Arc::new("payload".to_string());
cache.insert(Arc::new(7), Arc::clone(&value));
assert_eq!(cache.get(&Arc::new(7)).as_deref(), Some(&"payload".to_string()));

drop(value);
cache.clean_up(); // purges collected entries with RemovalCause::Collected
assert!(cache.get(&Arc::new(7)).is_none());
```

Weak keys compare by pointer identity, like Caffeine's `weakKeys`. Rust has no
tracing collector and no soft references, so dead entries are purged on access
or during `clean_up`; `soft_values()` is the explicit memory-pressure hook.
Removal listeners fire only when the key and value can still be upgraded.

### Asynchronous cache views

`AsyncCache` and `AsyncLoadingCache` mirror Caffeine's async views with Rust
`async` methods instead of `CompletableFuture`.

```rust
use caffeine::{AsyncCache, Cache};

let cache: Cache<u64, u64> = Cache::builder().maximum_size(10_000).build();
let async_cache = AsyncCache::new(cache);

let value = async_cache
    .get_with_async(42, |key| async move { key * 2 })
    .await;
assert_eq!(value, 84);
```

Concurrent misses for one key are coalesced: the first caller runs the
initializer and the others await its result. Cancellation, failure, or a panic
wakes the waiters and lets one of them retry; a failed load is never shared.
`AsyncLoadingCache::new(cache, loader)` installs a boxed async loader, and the
builder forms mirror Caffeine:

```rust
let async_cache: caffeine::AsyncCache<u64, u64> =
    caffeine::Cache::builder().maximum_size(1_000).build_async();
let loading = caffeine::Cache::builder()
    .maximum_size(1_000)
    .build_async_with_loader(|key: u64| async move { key * 2 });
```

`AsyncLoadingCache::get_all` loads every requested key; `AsyncCache::put_all`
and `as_map` complete the map-shaped surface. Async loading caches also honor
`refresh_after_write`: a stale read returns the old value and one background
thread per key reloads it, coalescing concurrent stale reads.

### Policy inspection

The policy views mirror Caffeine's `Policy` hierarchy.

```rust
use std::time::Duration;

let cache: caffeine::Cache<u64, u64> = caffeine::Cache::builder()
    .maximum_size(1_000)
    .expire_after_access(Duration::from_secs(30))
    .build();

let eviction = cache.policy().eviction().unwrap();
eviction.set_maximum(500);          // eagerly evicts the excess
let coldest = eviction.coldest(10);
let hottest = eviction.hottest(10);
let coldest_by_weight = eviction.coldest_weighted(100);
let hottest_by_weight = eviction.hottest_weighted(100);

let expiration = cache.policy().expire_after_access().unwrap();
expiration.set_expires_after(Duration::from_secs(60));
```

`expire_after_write()` returns the TTL view, `expire_variably()` returns the
per-entry view (`put`, `put_if_absent`, `set_expires_after`, `oldest`,
`youngest`), and `refresh_after_write()` exposes refresh aging and updates.
`get_if_present_quietly()` reads without recording statistics.

### Simulator

The `simulator` module replays access traces against this crate's W-TinyLFU
policy or reference implementations of LRU, LFU, FIFO, SLRU, CLOCK, and
Belady's optimal policy:

```rust
use caffeine::simulator::{simulate, Policy};

let stats = simulate([1, 2, 1, 3, 1], Policy::Wtinylfu, 2);
println!("hit rate {:.2}", stats.hit_rate());
```

### Guava and JCache adapters

The `guava` module provides a Guava-shaped builder, cache, and loading cache,
and the `jcache` module provides a JSR-107-shaped provider, manager,
configuration, and cache. `GuavaCache::from(cache)` and `Into<Cache>` convert
between the adapter and the native cache. They are Rust API-shape adapters over
this crate, not binary-compatible with the Java libraries.

## Equivalence with Java Caffeine

`examples/equivalence_trace.rs` and
`tools/equivalence/CaffeineEquivalence.java` replay the same 200,000-access
synthetic trace five times with a fresh cache per trial. CI aggregates the
trials and compares hit rate and eviction counts with bounded tolerances (2%
and 10%), guarding against policy drift while smoothing the randomized
frequency-sketch hashing used by both implementations.

## Architecture

```text
 key ──hash──► Mutex<Shard>          (one global policy)
                 ├── HashMap<K, node id>
                 ├── Window LRU          (adaptive, starts at 1%)
                 ├── Probation LRU       (20% of main)
                 ├── Protected LRU       (80% of main)
                 ├── 4-bit Count-Min sketch
                 ├── ordered expiry deadline index
                 └── weight accounting
```

**Admission.** New entries enter the window. When the window overflows, its
LRU entry becomes the admission candidate. If the main space has room it moves
into probation. Otherwise the candidate is compared with the probation LRU
victim; the higher estimated frequency survives. A probation hit promotes the
entry to protected; protected overflow demotes its LRU entry back to
probation.

**Frequency sketch.** Four rows of 4-bit saturating counters in one global
table, updated on every read and write. After `10 × capacity` increments the
counters are halved, so old popularity fades. Frequency is the minimum of the
four counters.

**Adaptive window.** Below 4096 entries the reactive hill-climber samples the
hit rate and walks the window size uphill: a 6.25% step (minimum 2 entries), a
0.98 decay (0.995 for caches up to 512 entries), and a restart when the hit
rate changes by 5% or more. Small caches take proportionally longer samples.
Above 4096 entries the density tier compares each region's hits per unit of
capacity, probes starved regions, audits stable positions, and keeps an anchor
to recover from sustained regressions. Both tiers are direct ports of
Caffeine's `WindowClimber`, including the starvation/audit ladders, retreat
budgets, and retry backoff. Window growth borrows from the protected segment
and transfers main-space entries into the window; shrinking reverses the
transfer. A single global climber drives the whole cache.

**One global policy.** Caffeine applies one admission window, one segmented
main space, and one frequency sketch across the entire cache; its policy state
sits behind a single eviction lock. This crate keeps that same global policy
under one mutex, so admission and eviction decisions match. The internal
concurrency architecture differs — Caffeine stripes its hash table and buffers
reads for higher multi-core throughput — which is a performance characteristic,
not a policy difference. Listeners, loaders, and refresh tasks run outside the
lock.

**Why no `unsafe`?** Nodes live in a slab (`Vec<Option<Box<Node>>>`) addressed
by `u32` ids, and LRU lists are intrusive `prev`/`next` indices. No raw
pointers are needed.

## Concurrency and semantics

- `Cache<K, V>` is a cloneable handle; every method takes `&self`.
- One mutex protects the whole cache. Listeners, loaders, and refresh tasks
  run after it is released; custom `Expiry` callbacks run while it is held.
- Listeners run synchronously in the removing thread. They may call back into
  the cache safely.
- `Expiry` callbacks must not call back into the cache; doing so deadlocks.
- `get` returns cloned values. Store `Arc<T>` when values are expensive to
  clone.
- Keys must be `Hash + Eq + Clone`; the policy keeps a copy for ordering.

## Testing

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo test --doc
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

The suite covers eviction bounds, scan resistance, TTL and idle expiration,
both fixed TTLs at once, variable per-entry expiry, reactive and density
window adaptation, coalesced concurrent loads, refresh coalescing and failure
recovery, listeners and causes, statistics, weighted eviction, zero-weight
entries, loading, iterator/retain behavior, builder validation, policy views
and resizing, weak/soft reference collection, async loading and retry,
simulator policies, the Guava/JCache adapters, and multi-threaded stress.
Time-dependent tests use a fake `Ticker`; async tests use a small `block_on`
helper and refresh tests use a queue `Executor`, so no sleeps are required.

CI runs on GitHub Actions across every push and pull request, including an
MSRV (Rust 1.75) check and the differential Java Caffeine job.

## License

Apache License 2.0. See [LICENSE](LICENSE).

This project is an independent Rust implementation inspired by
[Caffeine](https://github.com/ben-manes/caffeine), which is also Apache-2.0
licensed. It is not affiliated with or endorsed by the Caffeine project.
