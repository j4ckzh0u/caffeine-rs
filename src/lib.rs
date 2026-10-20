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
//! - Adaptive admission window: reactive hill climbing up to 4096 entries,
//!   density feedback with probes and audits above it
//! - Asynchronous refresh after write, with one in-flight refresh per key
//! - Removal and eviction listeners, with removal causes
//! - Hit, miss, load, and eviction statistics
//! - One global eviction policy under a lock, with no `unsafe` code
//!
//! ## Differences from Java Caffeine
//!
//! - Listeners run synchronously in the removing thread instead of on an
//!   executor.
//! - Weak keys/values and soft values are Java garbage-collector features.
//!   Rust ownership has no equivalent; store `Weak`/`Arc` values explicitly
//!   and remove dead entries yourself when that behavior is wanted.
//! - The asynchronous cache views are Java `CompletableFuture` adapters. The
//!   synchronous cache is `Send + Sync` and can be called from any async
//!   runtime; refresh already runs on a configurable executor.
//! - JCache, Guava, and simulator modules are Java-platform integrations and
//!   have no Rust counterpart.
//! - Capacity is a single global policy, matching Caffeine's eviction lock,
//!   rather than a striped approximation.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod builder;
mod cache;
mod climber;
mod config;
mod expiry;
mod frequency;
mod removal;
mod shard;
mod stats;
mod ticker;

pub use builder::CacheBuilder;
pub use cache::{Cache, Policy};
pub use expiry::{
    Expiry, ExpiryAccessing, ExpiryAfterAccess, ExpiryAfterWrite, ExpiryCreating, ExpiryWriting,
};
pub use removal::{EvictionListener, RemovalCause, RemovalListener};
pub use stats::CacheStats;
pub use ticker::{SystemTicker, Ticker};
