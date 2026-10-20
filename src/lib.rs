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
//! - Expiration after write or after access
//! - Asynchronous refresh after write, with one in-flight refresh per key
//! - Removal and eviction listeners, with removal causes
//! - Hit, miss, load, and eviction statistics
//! - Sharded, lock-based concurrency with no `unsafe` code
//!
//! ## Differences from Java Caffeine
//!
//! - Listeners run synchronously in the removing thread instead of on an
//!   executor.
//! - Weak keys/values and soft values are not implemented; Rust ownership
//!   makes them a different API. Store `Weak`/`Arc` values explicitly.
//! - Variable per-entry expiration (`Expiry`) and the async cache views are
//!   not implemented.
//! - The admission window is fixed at 1% rather than hill-climb adaptive.
//! - Capacity is sharded proportionally across locks, so retention is a
//!   close approximation of a single global policy rather than exact.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod builder;
mod cache;
mod config;
mod frequency;
mod removal;
mod shard;
mod stats;
mod ticker;

pub use builder::CacheBuilder;
pub use cache::{Cache, Policy};
pub use removal::{EvictionListener, RemovalCause, RemovalListener};
pub use stats::CacheStats;
pub use ticker::{SystemTicker, Ticker};
