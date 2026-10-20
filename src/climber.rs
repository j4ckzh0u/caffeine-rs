//! Adaptive admission window sizing via hill climbing.
//!
//! This is the reactive tier of Caffeine's adaptive W-TinyLFU window: it
//! samples the cache's hit rate, walks the window size uphill, reverses when
//! the hit rate falls, and decays its step as it converges. Very small caches
//! use longer samples and slower decay because their per-region counts are
//! noisy.

/// The hit-rate change large enough to restart the step at its initial size.
const RESTART_THRESHOLD: f64 = 0.05;
/// The initial step as a fraction of the cache's maximum.
const STEP_PERCENT: f64 = 0.0625;
/// The step decay rate for caches above the slow-adapt threshold.
const STEP_DECAY_RATE: f64 = 0.98;
/// Small caches use longer samples and slower decay.
const SLOW_ADAPT_THRESHOLD: u64 = 512;
const SLOW_ADAPT_RATIO_CAP: f64 = 4.0;
const SLOW_ADAPT_DECAY_RATE: f64 = 0.995;
const MIN_INITIAL_STEP: f64 = 2.0;

pub(crate) struct WindowClimber {
    sample: Sample,
    step: Step,
    adjustment: i64,
    slow_adapting: bool,
}

impl WindowClimber {
    pub(crate) fn new(maximum: u64) -> Self {
        let slow_adapting = maximum <= SLOW_ADAPT_THRESHOLD;
        Self {
            sample: Sample::default(),
            step: Step::new(maximum, slow_adapting),
            adjustment: 0,
            slow_adapting,
        }
    }

    pub(crate) fn record_miss(&mut self) {
        self.sample.record_miss();
    }

    pub(crate) fn record_hit(&mut self, in_window: bool, in_probation: bool) {
        self.sample.record_hit(in_window, in_probation);
    }

    pub(crate) fn adjustment(&self) -> i64 {
        self.adjustment
    }

    pub(crate) fn carry_over(&mut self, remaining: i64) {
        self.adjustment = remaining;
    }

    pub(crate) fn determine_adjustment(
        &mut self,
        maximum: u64,
        window_maximum: u64,
        main_protected_maximum: u64,
        sketch_sample_size: u64,
    ) {
        let _ = (window_maximum, main_protected_maximum);
        if self.sample.request_count() >= self.sample_period(maximum, sketch_sample_size) {
            self.adjustment = self.climb(maximum);
            self.sample.close();
        }
    }

    /// Discards a full sample gathered while the cache was too empty to adapt
    /// from, preserving its previous hit rate for the next comparison.
    pub(crate) fn discard_sample(&mut self, maximum: u64, sketch_sample_size: u64) {
        if self.sample.request_count() >= self.sample_period(maximum, sketch_sample_size) {
            self.sample.discard();
        }
    }

    fn sample_period(&self, maximum: u64, sketch_sample_size: u64) -> u64 {
        if !self.slow_adapting {
            return sketch_sample_size;
        }
        let initial_step = STEP_PERCENT * maximum as f64;
        let magnitude = self
            .step
            .magnitude()
            .max(initial_step / SLOW_ADAPT_RATIO_CAP);
        if magnitude == 0.0 {
            return sketch_sample_size;
        }
        let ratio = (initial_step / magnitude).clamp(1.0, SLOW_ADAPT_RATIO_CAP);
        if ratio == 1.0 {
            sketch_sample_size
        } else {
            (sketch_sample_size as f64 * ratio) as u64
        }
    }

    fn climb(&mut self, maximum: u64) -> i64 {
        let hit_rate_change = self.sample.hit_rate_change();
        let amount = self.step.heading(hit_rate_change >= 0.0);
        let next = if hit_rate_change.abs() >= RESTART_THRESHOLD {
            Step::restart_magnitude(maximum).copysign(amount)
        } else {
            self.decay_rate() * amount
        };
        self.step.commit(next);
        amount as i64
    }

    fn decay_rate(&self) -> f64 {
        if self.slow_adapting {
            SLOW_ADAPT_DECAY_RATE
        } else {
            STEP_DECAY_RATE
        }
    }
}

#[derive(Default)]
struct Sample {
    previous_hit_rate: f64,
    #[allow(dead_code)] // retained for the density tier's region metrics
    probation_hits: u64,
    #[allow(dead_code)]
    window_hits: u64,
    misses: u64,
    hits: u64,
}

impl Sample {
    fn record_hit(&mut self, in_window: bool, in_probation: bool) {
        self.hits += 1;
        if in_window {
            self.window_hits += 1;
        } else if in_probation {
            self.probation_hits += 1;
        }
    }

    fn record_miss(&mut self) {
        self.misses += 1;
    }

    fn request_count(&self) -> u64 {
        self.hits + self.misses
    }

    fn hit_rate(&self) -> f64 {
        self.hits as f64 / self.request_count() as f64
    }

    fn hit_rate_change(&self) -> f64 {
        self.hit_rate() - self.previous_hit_rate
    }

    fn close(&mut self) {
        let rate = self.hit_rate();
        self.close_at(rate);
    }

    fn discard(&mut self) {
        let rate = self.previous_hit_rate;
        self.close_at(rate);
    }

    fn close_at(&mut self, rate: f64) {
        self.previous_hit_rate = rate;
        self.probation_hits = 0;
        self.window_hits = 0;
        self.misses = 0;
        self.hits = 0;
    }
}

struct Step {
    size: f64,
}

impl Step {
    fn new(maximum: u64, grow: bool) -> Self {
        let magnitude = Self::restart_magnitude(maximum);
        Self {
            size: if grow { magnitude } else { -magnitude },
        }
    }

    fn commit(&mut self, step: f64) {
        self.size = step;
    }

    fn heading(&self, forward: bool) -> f64 {
        if forward {
            self.size
        } else {
            -self.size
        }
    }

    fn magnitude(&self) -> f64 {
        self.size.abs()
    }

    fn restart_magnitude(maximum: u64) -> f64 {
        (STEP_PERCENT * maximum as f64).max(MIN_INITIAL_STEP)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_caches_start_by_growing() {
        let mut climber = WindowClimber::new(10);
        for _ in 0..100 {
            climber.record_hit(true, false);
        }
        climber.determine_adjustment(10, 1, 7, 100);
        assert!(climber.adjustment() > 0);
    }

    #[test]
    fn falling_hit_rate_reverses_direction() {
        let mut climber = WindowClimber::new(10);
        for _ in 0..100 {
            climber.record_hit(true, false);
        }
        climber.determine_adjustment(10, 1, 7, 100);
        assert_eq!(climber.adjustment(), 2);

        for _ in 0..100 {
            climber.record_miss();
        }
        climber.determine_adjustment(10, 3, 5, 100);
        assert!(climber.adjustment() < 0);
    }

    #[test]
    fn no_adjustment_before_a_full_sample() {
        let mut climber = WindowClimber::new(100_000);
        for _ in 0..10 {
            climber.record_miss();
        }
        climber.determine_adjustment(100_000, 1_000, 79_200, 1_000_000);
        assert_eq!(climber.adjustment(), 0);
    }
}
