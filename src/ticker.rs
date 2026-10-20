//! Pluggable time source, mirroring Caffeine's `Ticker` interface.

use std::sync::Arc;
use std::time::Instant;

/// A monotonic time source returning nanoseconds.
///
/// Implement this to drive expiration deterministically in tests instead of
/// sleeping.
pub trait Ticker: Send + Sync + 'static {
    /// Returns the current time in nanoseconds from an arbitrary, fixed epoch.
    fn read(&self) -> u64;
}

/// The default ticker, backed by [`Instant`].
pub struct SystemTicker {
    start: Instant,
}

impl SystemTicker {
    /// Creates a ticker whose epoch is now.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Default for SystemTicker {
    fn default() -> Self {
        Self::new()
    }
}

impl Ticker for SystemTicker {
    fn read(&self) -> u64 {
        nanos(self.start.elapsed())
    }
}

pub(crate) fn nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

pub(crate) type SharedTicker = Arc<dyn Ticker>;
