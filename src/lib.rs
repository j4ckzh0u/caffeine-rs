//! # caffeine-rs
//!
//! A high-performance, near-optimal in-memory cache for Rust, inspired by
//! [Caffeine](https://github.com/ben-manes/caffeine). The eviction policy is
//! Window TinyLFU: a small LRU admission window backed by a segmented LRU main
//! space, with a 4-bit Count-Min frequency sketch deciding admissions.
//!
//! ## Example
//!
//! ```
//! use caffeine::Cache;
//! use std::time::Duration;
//!
//! let cache: Cache<String, u64> = Cache::builder()
//!     .maximum_size(10_000)
//!     .expire_after_write(Duration::from_secs(300))
//!     .record_stats(true)
//!     .build();
//!
//! cache.insert("answer".to_string(), 42);
//! assert_eq!(cache.get("answer"), Some(42));
//! assert_eq!(cache.stats().hit_count, 1);
//! ```
//!
//! ## Features
//!
//! - Size-based eviction with W-TinyLFU (`maximum_size`)
//! - Weight-based eviction with a custom weigher (`maximum_weight` + `weigher`)
//! - Expiration after write, after access, both at once, or per entry via [`Expiry`]
//! - Automatic loading (`get_loaded`, `get_all_loaded`) and a live map view
//!   (`as_map`)
//! - Adaptive admission window: reactive hill climbing up to 4096 entries,
//!   density feedback with probes and audits above it
//! - Asynchronous refresh after write, with one in-flight refresh per key
//! - Removal and eviction listeners, with removal causes
//! - Hit, miss, load, and eviction statistics
//! - Policy views for eviction, fixed and variable expiration, and refresh
//! - Weak keys/values and soft values via [`ReferenceCache`]
//! - Async cache views via [`AsyncCache`] and [`AsyncLoadingCache`]
//! - Guava-shaped and JCache-shaped adapters, plus a trace simulator
//! - One global eviction policy under a lock, with no `unsafe` code
//!
//! ## Differences from Java Caffeine
//!
//! - Listeners run synchronously in the removing thread instead of on an
//!   executor.
//! - Rust has no tracing collector or soft references. [`ReferenceCache`]
//!   stores `Weak` handles, compares weak keys by pointer identity, and purges
//!   collected entries on access or `clean_up`.
//! - Async cache views use Rust `async` methods rather than
//!   `CompletableFuture`; concurrent misses are still coalesced.
//! - The `guava` and `jcache` modules are Rust API-shape adapters, not
//!   binary-compatible Java bindings; `simulator` replays traces against the
//!   policies implemented in this crate.
//! - The policy is global, matching Caffeine's single eviction lock. Caffeine
//!   stripes its hash table and buffers reads internally; this crate holds the
//!   whole cache under one mutex. That is a throughput characteristic, not a
//!   difference in admission or eviction semantics.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod async_cache;
mod builder;
mod cache;
mod climber;
mod config;
mod expiry;
mod frequency;
mod reference;
mod removal;
mod shard;
mod stats;
mod ticker;

pub mod guava;
pub mod jcache;
pub mod simulator;

pub use async_cache::{AsyncCache, AsyncLoadingCache, BoxFuture};
pub use builder::CacheBuilder;
pub use cache::{
    Cache, CacheEntry, CacheMap, Eviction, FixedExpiration, FixedRefresh, Policy, VarExpiration,
};
pub use expiry::{
    Expiry, ExpiryAccessing, ExpiryAfterAccess, ExpiryAfterWrite, ExpiryCreating, ExpiryWriting,
};
pub use reference::{ReferenceCache, ReferenceCacheBuilder, ReferenceStrength};
pub use removal::{EvictionListener, RemovalCause, RemovalListener};
pub use stats::CacheStats;
pub use ticker::{SystemTicker, Ticker};
