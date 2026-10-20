# caffeine-rs

[![CI](https://github.com/j4ckzh0u/caffeine-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/j4ckzh0u/caffeine-rs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A high-performance, near-optimal in-memory cache for Rust, inspired by
[Ben Manes' Caffeine](https://github.com/ben-manes/caffeine).

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
| Both fixed TTLs at once | ❌ | write and access expiration are mutually exclusive here |
| Variable per-entry expiry (`Expiry`) | ✅ | create, update, and read hooks |
| Hill-climbing adaptive window | ✅ | reactive tier, resized per shard |
| Refresh after write | ✅ | async, one in-flight refresh per key |
| Automatic loading | ✅ | `get_with` / `try_get_with` / `build_with_loader` |
| Removal listener | ✅ | synchronous, gets the `RemovalCause` |
| Eviction listener | ✅ | only `RemovalCause::Size` removals |
| Statistics | ✅ | hits, misses, loads, load time, evictions |
| Custom ticker | ✅ | deterministic time in tests |
| Custom executor | ✅ | drives refresh tasks |
| Weak / soft references | ❌ | use explicit `Arc` / `Weak` values |
| Async cache views | ❌ | the sync cache is thread-safe |
| Density-tier window heuristics | ❌ | the reactive hill climber runs at every size |
| JCache / Guava / simulator modules | ❌ | Java-ecosystem integrations |

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

`get_with` may run the initializer more than once if several threads miss the
same key at the same time. The cache remains consistent; the last insert wins.
Send the work through a single-flight helper if duplicate computation matters.

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

## Architecture

```text
 key ──hash──► shard index ──► Mutex<Shard>
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

**Frequency sketch.** Four rows of 4-bit saturating counters, one table per
shard, updated on every read and write. After `10 × capacity` increments the
counters are halved, so old popularity fades. Frequency is the minimum of the
four counters.

**Adaptive window.** Caffeine's reactive hill-climber samples the hit rate and
walks the window size uphill: a 6.25% step (minimum 2 entries), a 0.98 decay
(0.995 for caches up to 512 entries), and a restart when the hit rate changes
by 5% or more. Small caches take proportionally longer samples. Window growth
borrows from the protected segment and transfers main-space entries into the
window; shrinking reverses the transfer. Each shard adapts independently, and
`policy().window_maximum()` reports the sum.

**Sharding.** The capacity is divided across a power-of-two number of shards,
each with its own lock, index, policy, and sketch. This keeps unrelated keys
from contending. Small caches collapse to one shard to avoid fragmentation.
Retention is a close approximation of a single global policy rather than an
exact global LRU/TinyLFU ordering.

**Why no `unsafe`?** Nodes live in a slab (`Vec<Option<Box<Node>>>`) addressed
by `u32` ids, and LRU lists are intrusive `prev`/`next` indices. No raw
pointers are needed.

## Concurrency and semantics

- `Cache<K, V>` is a cloneable handle; every method takes `&self`.
- One mutex is held per operation. Listeners, loaders, and refresh tasks run
  after it is released; custom `Expiry` callbacks run while it is held.
- Listeners run synchronously in the removing thread. They may call back into
  the cache safely.
- `Expiry` callbacks must not call back into the cache; doing so deadlocks.
- `get` returns cloned values. Store `Arc<T>` when values are expensive to
  clone.
- Keys must be `Hash + Eq + Clone`; the policy keeps a copy for ordering.
- Shard selection uses a randomly seeded hash, so adversarial key sets cannot
  trivially collapse into one lock.

## Testing

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo test --doc
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

The suite covers eviction bounds, scan resistance, TTL and idle expiration,
variable per-entry expiry, adaptive-window growth, refresh coalescing and
failure recovery, listeners and causes, statistics, weighted eviction,
loading, iterator/retain behavior, builder validation, and multi-threaded
stress. Time-dependent tests use a fake `Ticker`; refresh tests use a queue
`Executor`, so no sleeps are required.

CI runs on GitHub Actions across every push and pull request, including an
MSRV (Rust 1.75) check.

## License

Apache License 2.0. See [LICENSE](LICENSE).

This project is an independent Rust implementation inspired by
[Caffeine](https://github.com/ben-manes/caffeine), which is also Apache-2.0
licensed. It is not affiliated with or endorsed by the Caffeine project.
