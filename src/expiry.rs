//! Variable per-entry expiration, mirroring Caffeine's `Expiry` interface.
//!
//! An `Expiry` calculates how long an entry may live after creation, update,
//! or read. Durations are measured against the cache's [`Ticker`], not wall
//! clock time. Returning `None` means the entry never expires.
//!
//! The `current` argument is the entry's **remaining** lifetime at the moment
//! of the callback, matching Caffeine's `currentDuration`. Returning it
//! unchanged preserves the existing deadline; the helpers below do that where
//! appropriate.

use std::sync::Arc;
use std::time::Duration;

use crate::ticker::nanos;

/// The sentinel deadline for an entry that never expires.
pub(crate) const NEVER: u64 = u64::MAX;

/// Caffeine caps a single expiry at roughly 150 years to keep deadline math
/// far away from overflow.
pub(crate) const MAXIMUM_EXPIRY: u64 = (i64::MAX as u64) >> 1;

/// Calculates the lifetime of a cache entry.
///
/// Callbacks run while the owning shard's lock is held and must not call back
/// into the cache; doing so deadlocks. Keep them cheap and side-effect free
/// where possible.
pub trait Expiry<K, V>: Send + Sync + 'static {
    /// Returns the lifetime after the entry is created, or `None` for no
    /// expiration.
    fn expire_after_create(&self, key: &K, value: &V, now: u64) -> Option<Duration>;

    /// Returns the lifetime after the entry is replaced. The default keeps the
    /// current expiry, matching Caffeine's creation-only semantics.
    fn expire_after_update(
        &self,
        _key: &K,
        _value: &V,
        _now: u64,
        current: Option<Duration>,
    ) -> Option<Duration> {
        current
    }

    /// Returns the lifetime after the entry is read. The default keeps the
    /// current expiry, matching Caffeine's creation/write-only semantics.
    fn expire_after_read(
        &self,
        _key: &K,
        _value: &V,
        _now: u64,
        current: Option<Duration>,
    ) -> Option<Duration> {
        current
    }
}

/// A fixed lifetime after creation or replacement; reads do not extend it.
pub struct ExpiryAfterWrite(Duration);

impl ExpiryAfterWrite {
    /// Creates a fixed after-write expiry.
    pub fn new(duration: Duration) -> Self {
        Self(duration)
    }
}

impl<K, V> Expiry<K, V> for ExpiryAfterWrite {
    fn expire_after_create(&self, _key: &K, _value: &V, _now: u64) -> Option<Duration> {
        Some(self.0)
    }

    fn expire_after_update(
        &self,
        _key: &K,
        _value: &V,
        _now: u64,
        _current: Option<Duration>,
    ) -> Option<Duration> {
        Some(self.0)
    }
}

/// A fixed idle timeout after creation, replacement, or read.
pub struct ExpiryAfterAccess(Duration);

impl ExpiryAfterAccess {
    /// Creates a fixed after-access expiry.
    pub fn new(duration: Duration) -> Self {
        Self(duration)
    }
}

impl<K, V> Expiry<K, V> for ExpiryAfterAccess {
    fn expire_after_create(&self, _key: &K, _value: &V, _now: u64) -> Option<Duration> {
        Some(self.0)
    }

    fn expire_after_update(
        &self,
        _key: &K,
        _value: &V,
        _now: u64,
        _current: Option<Duration>,
    ) -> Option<Duration> {
        Some(self.0)
    }

    fn expire_after_read(
        &self,
        _key: &K,
        _value: &V,
        _now: u64,
        _current: Option<Duration>,
    ) -> Option<Duration> {
        Some(self.0)
    }
}

/// A creation-only variable expiry. Updates and reads keep the existing
/// deadline.
pub struct ExpiryCreating<F>(F);

impl<F> ExpiryCreating<F> {
    /// Creates an expiry from the supplied calculator.
    pub fn new(calculator: F) -> Self {
        Self(calculator)
    }
}

impl<K, V, F> Expiry<K, V> for ExpiryCreating<F>
where
    F: Fn(&K, &V) -> Option<Duration> + Send + Sync + 'static,
{
    fn expire_after_create(&self, key: &K, value: &V, _now: u64) -> Option<Duration> {
        (self.0)(key, value)
    }
}

/// A variable after-write expiry. A replacement recalculates the lifetime;
/// reads keep the existing deadline.
pub struct ExpiryWriting<F>(F);

impl<F> ExpiryWriting<F> {
    /// Creates an expiry from the supplied calculator.
    pub fn new(calculator: F) -> Self {
        Self(calculator)
    }
}

impl<K, V, F> Expiry<K, V> for ExpiryWriting<F>
where
    F: Fn(&K, &V) -> Option<Duration> + Send + Sync + 'static,
{
    fn expire_after_create(&self, key: &K, value: &V, _now: u64) -> Option<Duration> {
        (self.0)(key, value)
    }

    fn expire_after_update(
        &self,
        key: &K,
        value: &V,
        _now: u64,
        _current: Option<Duration>,
    ) -> Option<Duration> {
        (self.0)(key, value)
    }
}

/// A variable after-access expiry. Creation, replacement, and reads all
/// recalculate the lifetime.
pub struct ExpiryAccessing<F>(F);

impl<F> ExpiryAccessing<F> {
    /// Creates an expiry from the supplied calculator.
    pub fn new(calculator: F) -> Self {
        Self(calculator)
    }
}

impl<K, V, F> Expiry<K, V> for ExpiryAccessing<F>
where
    F: Fn(&K, &V) -> Option<Duration> + Send + Sync + 'static,
{
    fn expire_after_create(&self, key: &K, value: &V, _now: u64) -> Option<Duration> {
        (self.0)(key, value)
    }

    fn expire_after_update(
        &self,
        key: &K,
        value: &V,
        _now: u64,
        _current: Option<Duration>,
    ) -> Option<Duration> {
        (self.0)(key, value)
    }

    fn expire_after_read(
        &self,
        key: &K,
        value: &V,
        _now: u64,
        _current: Option<Duration>,
    ) -> Option<Duration> {
        (self.0)(key, value)
    }
}

/// The cache's internal expiration strategy.
pub(crate) enum Expiration<K, V> {
    None,
    AfterWrite(u64),
    AfterAccess(u64),
    Custom(Arc<dyn Expiry<K, V>>),
}

impl<K, V> Clone for Expiration<K, V> {
    fn clone(&self) -> Self {
        match self {
            Self::None => Self::None,
            Self::AfterWrite(ttl) => Self::AfterWrite(*ttl),
            Self::AfterAccess(ttl) => Self::AfterAccess(*ttl),
            Self::Custom(expiry) => Self::Custom(Arc::clone(expiry)),
        }
    }
}

impl<K, V> Expiration<K, V>
where
    K: 'static,
    V: 'static,
{
    pub(crate) fn fixed_write(&self) -> Option<u64> {
        match self {
            Self::AfterWrite(ttl) => Some(*ttl),
            _ => None,
        }
    }

    pub(crate) fn fixed_access(&self) -> Option<u64> {
        match self {
            Self::AfterAccess(ttl) => Some(*ttl),
            _ => None,
        }
    }

    pub(crate) fn is_custom(&self) -> bool {
        matches!(self, Self::Custom(_))
    }

    pub(crate) fn on_create(&self, key: &K, value: &V, now: u64) -> u64 {
        match self {
            Self::None => NEVER,
            Self::AfterWrite(ttl) | Self::AfterAccess(ttl) => deadline(now, Some(*ttl)),
            Self::Custom(expiry) => {
                deadline_from_duration(now, expiry.expire_after_create(key, value, now))
            }
        }
    }

    pub(crate) fn on_write(&self, key: &K, value: &V, now: u64, current: u64) -> u64 {
        match self {
            Self::None => NEVER,
            Self::AfterWrite(ttl) | Self::AfterAccess(ttl) => deadline(now, Some(*ttl)),
            Self::Custom(expiry) => deadline_from_duration(
                now,
                expiry.expire_after_update(key, value, now, remaining(current, now)),
            ),
        }
    }

    pub(crate) fn on_read(&self, key: &K, value: &V, now: u64, current: u64) -> u64 {
        match self {
            Self::None => NEVER,
            Self::AfterWrite(_) => current,
            Self::AfterAccess(ttl) => deadline(now, Some(*ttl)),
            Self::Custom(expiry) => deadline_from_duration(
                now,
                expiry.expire_after_read(key, value, now, remaining(current, now)),
            ),
        }
    }
}

fn deadline(now: u64, duration: Option<u64>) -> u64 {
    match duration {
        // Keep a real deadline distinct from the NEVER sentinel even when the
        // clock is close to u64::MAX.
        Some(duration) => now
            .saturating_add(duration.min(MAXIMUM_EXPIRY))
            .min(NEVER - 1),
        None => NEVER,
    }
}

fn deadline_from_duration(now: u64, duration: Option<Duration>) -> u64 {
    deadline(now, duration.map(nanos))
}

fn remaining(deadline: u64, now: u64) -> Option<Duration> {
    if deadline == NEVER {
        None
    } else {
        Some(Duration::from_nanos(deadline.saturating_sub(now)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_write_keeps_deadline_on_read() {
        let expiry: Expiration<u64, u64> = Expiration::AfterWrite(5);
        let created = expiry.on_create(&1, &1, 0);
        assert_eq!(created, 5);
        assert_eq!(expiry.on_read(&1, &1, 3, created), 5);
        assert_eq!(expiry.on_write(&1, &1, 3, created), 8);
    }

    #[test]
    fn fixed_access_extends_deadline_on_read() {
        let expiry: Expiration<u64, u64> = Expiration::AfterAccess(5);
        let created = expiry.on_create(&1, &1, 0);
        assert_eq!(expiry.on_read(&1, &1, 3, created), 8);
    }

    #[test]
    fn deadline_never_collides_with_the_never_sentinel() {
        assert_eq!(deadline(0, Some(5)), 5);
        assert_eq!(deadline(0, None), NEVER);
        assert_ne!(deadline(u64::MAX, Some(1)), NEVER);
    }

    #[test]
    fn custom_read_receives_remaining_time() {
        use std::sync::{Arc, Mutex};

        struct Probe(Arc<Mutex<Vec<Option<Duration>>>>);

        impl Expiry<u64, u64> for Probe {
            fn expire_after_create(&self, _key: &u64, _value: &u64, _now: u64) -> Option<Duration> {
                Some(Duration::from_secs(10))
            }

            fn expire_after_read(
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

        let seen = Arc::new(Mutex::new(Vec::new()));
        let expiry = Expiration::Custom(Arc::new(Probe(Arc::clone(&seen))));
        let created = expiry.on_create(&1, &1, 0);
        assert_eq!(created, 10_000_000_000);
        assert_eq!(expiry.on_read(&1, &1, 4_000_000_000, created), created);
        assert_eq!(seen.lock().unwrap()[0], Some(Duration::from_secs(6)));
    }
}
