//! Adaptive admission window sizing via hill climbing.
//!
//! Caffeine selects one of two tiers. Small caches compare hit rates across
//! samples and walk the window uphill. Caches above 4096 entries instead
//! compare each region's hit density within a sample, using probes to explore
//! starved regions, audits to test stable positions, and an anchor to recover
//! from sustained regressions.

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
    adjustment: i64,
    tier: Tier,
}

enum Tier {
    Reactive(ReactiveClimber),
    Density(Box<DensityClimber>),
}

impl WindowClimber {
    pub(crate) fn new(maximum: u64) -> Self {
        let tier = if DensityClimber::applies_to(maximum) {
            Tier::Density(Box::new(DensityClimber::new(maximum)))
        } else {
            Tier::Reactive(ReactiveClimber::new(maximum))
        };
        Self {
            sample: Sample::default(),
            adjustment: 0,
            tier,
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

    /// Discards the current sample and the previous hit rate.
    pub(crate) fn reset_sample(&mut self) {
        self.sample.reset();
    }

    pub(crate) fn determine_adjustment(
        &mut self,
        maximum: u64,
        window_maximum: u64,
        main_protected_maximum: u64,
        sketch_sample_size: u64,
    ) {
        if self.sample.request_count() >= self.sample_period(maximum, sketch_sample_size) {
            let amount = match &mut self.tier {
                Tier::Reactive(climber) => climber.climb(&self.sample, maximum),
                Tier::Density(climber) => climber.climb(
                    &self.sample,
                    maximum,
                    window_maximum,
                    main_protected_maximum,
                ),
            };
            self.adjustment = amount as i64;
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
        match &self.tier {
            Tier::Reactive(climber) => climber.sample_period(maximum, sketch_sample_size),
            Tier::Density(climber) => climber.sample_period(maximum, sketch_sample_size),
        }
    }
}

struct ReactiveClimber {
    step: Step,
    slow_adapting: bool,
}

impl ReactiveClimber {
    fn new(maximum: u64) -> Self {
        let slow_adapting = maximum <= SLOW_ADAPT_THRESHOLD;
        Self {
            step: Step::new(maximum, slow_adapting),
            slow_adapting,
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

    fn climb(&mut self, sample: &Sample, maximum: u64) -> f64 {
        let hit_rate_change = sample.hit_rate_change();
        let amount = self.step.heading(hit_rate_change >= 0.0);
        let next = if hit_rate_change.abs() >= RESTART_THRESHOLD {
            Step::restart_magnitude(maximum).copysign(amount)
        } else {
            self.decay_rate() * amount
        };
        self.step.commit(next);
        amount
    }

    fn decay_rate(&self) -> f64 {
        if self.slow_adapting {
            SLOW_ADAPT_DECAY_RATE
        } else {
            STEP_DECAY_RATE
        }
    }
}

/// The tier above the density threshold: steers by hits per unit of capacity,
/// probes starved regions, audits stable positions, and keeps an anchor.
struct DensityClimber {
    audit_clock: AuditClock,
    starvation: Ladder,
    anchor: Anchor,
    audit: Ladder,
    rates: Rates,
    walk: Option<Walk>,
    undo_remaining: i64,
    refractory_left: i32,
    retreat_left: i32,
    step: Step,
}

impl DensityClimber {
    /// The cache size threshold between reactive and density feedback.
    const DENSITY_THRESHOLD: u64 = 4096;
    /// A longer sample period stabilizes density estimates.
    const SAMPLE_MULTIPLIER: u64 = 4;
    /// The step per unit of log density-ratio error, as a fraction of the maximum.
    const DENSITY_GAIN: f64 = 0.03;
    /// The samples covered by a retreat stride, including its arrival sample.
    const RETREAT_COVER: i32 = 2;

    fn new(maximum: u64) -> Self {
        Self {
            audit_clock: AuditClock::new(),
            starvation: Ladder::new(),
            anchor: Anchor::new(),
            audit: Ladder::new(),
            rates: Rates::new(),
            walk: None,
            undo_remaining: 0,
            refractory_left: 0,
            retreat_left: 0,
            step: Step::new(maximum, false),
        }
    }

    fn applies_to(maximum: u64) -> bool {
        maximum > Self::DENSITY_THRESHOLD
    }

    fn sample_period(&self, maximum: u64, sketch_sample_size: u64) -> u64 {
        let period = maximum.saturating_mul(Self::SAMPLE_MULTIPLIER);
        period.min(sketch_sample_size)
    }

    fn climb(
        &mut self,
        sample: &Sample,
        maximum: u64,
        window_max: u64,
        main_protected_max: u64,
    ) -> f64 {
        let reading = Reading::new(sample, maximum, window_max, main_protected_max);
        let amount = self.route(&reading);
        self.audit_clock.tick(reading.window_max, reading.band);
        amount
    }

    fn ladder_mut(&mut self, is_audit: bool) -> &mut Ladder {
        if is_audit {
            &mut self.audit
        } else {
            &mut self.starvation
        }
    }

    fn ladder(&self, is_audit: bool) -> &Ladder {
        if is_audit {
            &self.audit
        } else {
            &self.starvation
        }
    }

    /// Returns the next adjustment, giving probes and recovery priority over
    /// density steering.
    fn route(&mut self, reading: &Reading) -> f64 {
        self.age_park_shield();
        self.age_retreat_cover();

        if self.is_workload_shift(reading) && self.anchor.stand_down(reading) {
            // Discard the old workload's rate history along with its anchor.
            self.rates.reset();
        }
        self.update_rate_references(reading);

        if let Some(mut walk) = self.walk.take() {
            let ending = self.probe_ending(&mut walk, reading);
            if ending == ProbeEnding::Walking {
                let (amount, alive) = self.walk_step(&mut walk, false, reading);
                if alive {
                    self.walk = Some(walk);
                }
                return amount;
            }
            if ending != ProbeEnding::Confirmed {
                return self.undo_probe(&mut walk, ending, reading);
            }
            if self.keep_confirmed_position(&walk, reading) {
                return if self.anchor.returning {
                    self.stride_home(reading)
                } else {
                    0.0
                };
            }
            // A starvation confirm resumes density steering in the same sample.
            return self.steer(reading.steering_error(), reading);
        }

        if self.has_pending_undo() {
            return self.undo_stride(reading);
        }
        if self.anchor.returning {
            return self.stride_home(reading);
        }
        if self.anchor.is_retest_due(reading) {
            self.retest_return(reading);
            return 0.0;
        }
        if reading.has_blind_corner() {
            return if self.is_backing_off() {
                self.hold_or_audit(reading)
            } else {
                self.arm_starvation_probe(reading)
            };
        }
        if self.anchor.veto_triggered(reading, &self.rates) {
            return self.stride_home(reading);
        }
        if self.audit_clock.is_due() {
            return self.arm_equilibrium_audit(reading);
        }
        if self.anchor.held {
            // Density would move away from this better position; wait for an
            // audit to explore again.
            return 0.0;
        }
        self.steer(reading.steering_error(), reading)
    }

    fn age_park_shield(&mut self) {
        if self.is_shielded() {
            self.anchor.age_shield();
        }
    }

    fn age_retreat_cover(&mut self) {
        if self.retreat_left > 0 {
            self.retreat_left -= 1;
        }
    }

    fn is_shielded(&self) -> bool {
        self.walk.is_none() && self.anchor.is_shielded()
    }

    fn is_workload_shift(&self, reading: &Reading) -> bool {
        (reading.hit_rate_change.abs() >= RESTART_THRESHOLD)
            && !self.is_shielded()
            && !self.is_park_test()
            && !self.is_return_test()
    }

    fn is_return_test(&self) -> bool {
        (self.retreat_left > 0) || ((self.anchor.retest_claim >= 0.0) && !self.anchor.returning)
    }

    fn is_park_test(&self) -> bool {
        self.anchor.held && self.walk.as_ref().is_some_and(|walk| walk.is_audit)
    }

    fn update_rate_references(&mut self, reading: &Reading) {
        if self.rates.is_unseeded() {
            self.rates.seed(reading.hit_rate);
        } else {
            self.rates.update(reading.hit_rate);
            let probing = self.is_probing();
            self.anchor.track(reading, &self.rates, probing);
        }
    }

    fn is_probing(&self) -> bool {
        self.walk.is_some() || self.has_pending_undo()
    }

    fn has_pending_undo(&self) -> bool {
        self.undo_remaining != 0
    }

    fn undo_stride(&mut self, reading: &Reading) -> f64 {
        let stride = reading.capped_stride(self.undo_remaining as f64);
        self.undo_remaining -= stride as i64;
        self.retreat_left = Self::RETREAT_COVER;
        self.step.commit(stride)
    }

    fn stride_home(&mut self, reading: &Reading) -> f64 {
        let stride = self.anchor.stride_home(reading);
        self.step.commit(stride)
    }

    fn retest_return(&mut self, reading: &Reading) {
        if self.anchor.retest_fails(&self.rates) {
            self.rates.reset();
            self.anchor.stand_down(reading);
        }
    }

    fn is_backing_off(&self) -> bool {
        self.refractory_left > 0
    }

    fn hold_or_audit(&mut self, reading: &Reading) -> f64 {
        if self.audit_clock.is_due() {
            self.arm_equilibrium_audit(reading)
        } else {
            self.hold_in_refractory(reading)
        }
    }

    fn hold_in_refractory(&mut self, reading: &Reading) -> f64 {
        self.refractory_left -= 1;
        reading.at_least_floor(0.0)
    }

    fn arm_starvation_probe(&mut self, reading: &Reading) -> f64 {
        let mut walk = self.arm_probe(reading, reading.should_probe_down(), false);
        let (amount, alive) = self.walk_step(&mut walk, true, reading);
        if alive {
            self.walk = Some(walk);
        }
        amount
    }

    fn arm_equilibrium_audit(&mut self, reading: &Reading) -> f64 {
        let stride = self.audit.stride(reading);
        let down = self.audit_clock.choose_direction(
            reading,
            stride,
            self.rates.smoothed,
            self.anchor.held,
        );
        let mut walk = self.arm_probe(reading, down, true);
        self.audit_clock.restart();
        let (amount, alive) = self.walk_step(&mut walk, true, reading);
        if alive {
            self.walk = Some(walk);
        }
        amount
    }

    fn keep_confirmed_position(&mut self, walk: &Walk, reading: &Reading) -> bool {
        let park = walk.is_audit || walk.is_audit_grade(reading);
        let position = walk.verdict_window(reading);
        self.anchor.plant(position, self.rates.smoothed);
        if park {
            self.anchor.park(AuditClock::AUDIT_WAIT_INITIAL);
            if position != reading.window_max {
                self.anchor.begin_return();
            }
        } else {
            self.anchor.release();
        }
        park
    }

    fn arm_probe(&self, reading: &Reading, down: bool, is_audit: bool) -> Walk {
        Walk::new(
            is_audit,
            down,
            reading.window_max,
            reading.request_count,
            reading.hit_rate,
            self.rates.smoothed,
            reading.probation_density,
        )
    }

    /// Returns the step and whether the walk survives. Crossing back through
    /// the starting position ends the probe as a failure.
    fn walk_step(&mut self, walk: &mut Walk, entry: bool, reading: &Reading) -> (f64, bool) {
        let mut stride = self.next_stride(walk, entry, reading);
        if walk.crosses_base(reading.window_max as f64 + stride) {
            self.ladder_mut(walk.is_audit).crash_streak = 0;
            return (self.undo_probe(walk, ProbeEnding::Failed, reading), false);
        }
        if (stride < 0.0) && ((reading.window_max as f64 + stride) < reading.floor) {
            stride = reading.floored_descent();
        }
        walk.samples += 1;
        (self.step.commit(stride), true)
    }

    fn next_stride(&mut self, walk: &Walk, entry: bool, reading: &Reading) -> f64 {
        let magnitude = self.ladder(walk.is_audit).stride(reading);
        if entry {
            return walk.direction() * magnitude;
        }
        if walk.is_audit && (walk.below_bar_streak > 0) {
            let held = Step::decayed(self.step.size);
            return if Step::is_frozen(held) {
                self.step.at_minimum()
            } else {
                held
            };
        }
        let bar = walk.reversal_bar(&self.rates);
        let hit_rate_change = reading.hit_rate_change;
        let mut stride = self.step.heading(hit_rate_change > -bar);
        stride = if hit_rate_change.abs() >= bar {
            magnitude.copysign(stride)
        } else {
            Step::decayed(stride)
        };
        if Step::is_frozen(stride) {
            walk.direction() * magnitude
        } else {
            stride
        }
    }

    fn probe_ending(&mut self, walk: &mut Walk, reading: &Reading) -> ProbeEnding {
        let below_bar = reading.hit_rate <= (walk.base_hit_rate - walk.crash_bar(&self.rates));
        walk.below_bar_streak = if below_bar {
            walk.below_bar_streak + 1
        } else {
            0
        };
        if walk.should_crash_abort(below_bar, self.ladder(walk.is_audit).has_crashed()) {
            // A workload change can resemble probe damage. Retry the first
            // crash without increasing backoff.
            self.ladder_mut(walk.is_audit).crash();
            return ProbeEnding::Crashed;
        }
        let above = reading.hit_rate > (walk.base_smoothed_rate + VETO_MARGIN_MIN);
        walk.above_streak = if above { walk.above_streak + 1 } else { 0 };
        walk.remember_best(above, reading);
        walk.beat_base |= reading.hit_rate >= walk.base_hit_rate;
        if walk.is_audit {
            self.audit_ending(walk)
        } else {
            self.starvation_ending(walk, reading)
        }
    }

    fn audit_ending(&mut self, walk: &mut Walk) -> ProbeEnding {
        if walk.is_confirmed() {
            self.audit.reset();
            self.refractory_left = 0;
            self.starvation.reward();
            self.audit_clock.settle(walk.down, self.rates.smoothed);
            ProbeEnding::Confirmed
        } else if walk.is_budget_spent() {
            self.audit.crash_streak = 0;
            ProbeEnding::Failed
        } else {
            ProbeEnding::Walking
        }
    }

    fn starvation_ending(&mut self, walk: &mut Walk, reading: &Reading) -> ProbeEnding {
        if walk.can_adjudicate(reading, self.starvation.commitment_depth()) {
            self.starvation.crash_streak = 0;
            if (walk.verdict_signal(reading) * walk.direction()) > 0.0 {
                if walk.is_reversed_by(reading)
                    || self
                        .starvation
                        .is_repeat(walk.down, reading.window_max, reading.band)
                {
                    self.starvation.escalate();
                } else {
                    self.starvation.reward();
                }
                self.starvation.remember(walk.down, reading.window_max);
                self.refractory_left = 0;
                ProbeEnding::Confirmed
            } else {
                ProbeEnding::Failed
            }
        } else if walk.is_budget_spent() {
            self.starvation.crash_streak = 0;
            ProbeEnding::Failed
        } else {
            ProbeEnding::Walking
        }
    }

    fn undo_probe(&mut self, walk: &mut Walk, ending: ProbeEnding, reading: &Reading) -> f64 {
        let crashed = ending == ProbeEnding::Crashed;
        let failed = !crashed || self.ladder(walk.is_audit).crash_escalates();
        if failed {
            self.ladder_mut(walk.is_audit).escalate();
        }
        if walk.is_audit {
            let rung = self.audit.rung;
            self.audit_clock.reschedule(failed, crashed, rung);
        } else {
            self.refractory_left = self.starvation.rung;
            self.starvation.forget();
        }
        self.retreat_left = Self::RETREAT_COVER;
        self.return_to_base(walk, reading)
    }

    fn return_to_base(&mut self, walk: &Walk, reading: &Reading) -> f64 {
        let amount = walk.base_window as i64 - reading.window_max as i64;
        let stride = reading.capped_stride(amount as f64);
        self.undo_remaining = amount - stride as i64;
        self.step.commit(stride)
    }

    fn steer(&mut self, error: f64, reading: &Reading) -> f64 {
        let magnitude = reading
            .max_step()
            .min(error.abs() * Self::DENSITY_GAIN * reading.maximum as f64);
        let stride = if error >= 0.0 { magnitude } else { -magnitude };
        let clamped = if (stride < 0.0) && ((reading.window_max as f64 + stride) < reading.floor) {
            reading.floored_descent()
        } else {
            stride
        };
        let value = reading.at_least_floor(clamped);
        self.step.commit(value)
    }
}

#[derive(Default)]
struct Sample {
    previous_hit_rate: f64,
    probation_hits: u64,
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

    fn reset(&mut self) {
        self.close_at(0.0);
    }

    fn close_at(&mut self, rate: f64) {
        self.previous_hit_rate = rate;
        self.probation_hits = 0;
        self.window_hits = 0;
        self.misses = 0;
        self.hits = 0;
    }
}

/// The measurements and region bounds used for one density adjustment.
struct Reading {
    probation_density: f64,
    hit_rate_change: f64,
    window_density: f64,
    main_density: f64,
    hit_rate: f64,
    floor: f64,
    window_starved: bool,
    main_starved: bool,
    request_count: u64,
    window_hits: u64,
    window_max: u64,
    main_hits: u64,
    maximum: u64,
    band: u64,
    bar: u64,
}

impl Reading {
    /// The minimum hit count used for steering, as a fraction of the bar.
    const STEERING_FLOOR_FRACTION: f64 = 0.125;
    /// The band within which two positions count as the same.
    const STABLE_BAND_FRACTION: f64 = 0.02;
    /// The density law's lower bound on the window.
    const WINDOW_FLOOR_FRACTION: f64 = 0.02;
    /// The density climber's largest single step.
    const MAX_STEP_FRACTION: f64 = 0.30;
    /// Keeps a starved region's density ratio defined.
    const DENSITY_EPSILON: f64 = 1e-9;
    /// The minimum hit count needed to measure a region.
    const MIN_STARVATION_BAR: u64 = 4;
    /// The right-shift of the request count that sets the starvation bar.
    const MIN_SIGNAL_SHIFT: u32 = 10;

    fn new(sample: &Sample, maximum: u64, window_max: u64, main_protected_max: u64) -> Self {
        let main_hits = sample.hits - sample.window_hits;
        let bar = Self::MIN_STARVATION_BAR.max(sample.request_count() >> Self::MIN_SIGNAL_SHIFT);
        Self {
            main_density: main_hits as f64 / maximum.saturating_sub(window_max).max(1) as f64,
            window_density: sample.window_hits as f64 / window_max.max(1) as f64,
            probation_density: sample.probation_hits as f64
                / maximum
                    .saturating_sub(window_max)
                    .saturating_sub(main_protected_max)
                    .max(1) as f64,
            window_starved: sample.window_hits < bar,
            hit_rate_change: sample.hit_rate_change(),
            floor: Self::WINDOW_FLOOR_FRACTION * maximum as f64,
            request_count: sample.request_count(),
            main_starved: main_hits < bar,
            window_hits: sample.window_hits,
            main_hits,
            hit_rate: sample.hit_rate(),
            band: Self::stable_band(maximum),
            window_max,
            maximum,
            bar,
        }
    }

    fn stable_band(maximum: u64) -> u64 {
        1.max((Self::STABLE_BAND_FRACTION * maximum as f64) as u64)
    }

    /// The smallest window that leaves main at most a quarter of the cache.
    fn upper_corner(&self) -> u64 {
        self.maximum - (self.maximum >> 2)
    }

    fn max_step(&self) -> f64 {
        Self::MAX_STEP_FRACTION * self.maximum as f64
    }

    fn capped_stride(&self, amount: f64) -> f64 {
        let cap = self.max_step();
        if amount.abs() > cap {
            cap.copysign(amount)
        } else {
            amount
        }
    }

    fn restart_magnitude(&self) -> f64 {
        Step::restart_magnitude(self.maximum)
    }

    /// A descent limited by the floor, preserving negative zero to retain its
    /// direction.
    fn floored_descent(&self) -> f64 {
        let descent = self.floor - self.window_max as f64;
        if (self.window_max as f64) >= self.floor {
            if descent == 0.0 {
                -0.0
            } else {
                descent
            }
        } else {
            -0.0
        }
    }

    fn at_least_floor(&self, stride: f64) -> f64 {
        if (self.window_max as f64) < self.floor {
            stride.max(self.floor - self.window_max as f64)
        } else {
            stride
        }
    }

    fn is_dead_sample(&self) -> bool {
        self.window_starved && self.main_starved
    }

    fn has_blind_corner(&self) -> bool {
        self.is_dead_sample() || (self.window_starved && (self.window_max <= (self.maximum >> 2)))
    }

    fn should_probe_down(&self) -> bool {
        self.is_dead_sample() && (self.window_max >= (self.maximum >> 1))
    }

    fn error(&self) -> f64 {
        ((self.window_density + Self::DENSITY_EPSILON)
            / (self.main_density + Self::DENSITY_EPSILON))
            .ln()
    }

    fn steering_error(&self) -> f64 {
        let window_floor =
            (Self::STEERING_FLOOR_FRACTION * self.bar as f64) / self.window_max.max(1) as f64;
        let main_floor = (Self::STEERING_FLOOR_FRACTION * self.bar as f64)
            / self.maximum.saturating_sub(self.window_max).max(1) as f64;
        (self.window_density.max(window_floor) / self.main_density.max(main_floor)).ln()
    }
}

/// The signed adjustment used to continue, reverse, or decay the next step.
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

    fn commit(&mut self, step: f64) -> f64 {
        self.size = step;
        step
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

    fn at_minimum(&self) -> f64 {
        MIN_INITIAL_STEP.copysign(self.size)
    }

    fn restart_magnitude(maximum: u64) -> f64 {
        (STEP_PERCENT * maximum as f64).max(MIN_INITIAL_STEP)
    }

    fn decayed(step: f64) -> f64 {
        STEP_DECAY_RATE * step
    }

    fn is_frozen(step: f64) -> bool {
        step.abs() < MIN_INITIAL_STEP
    }
}

/// A bounded sequence of window adjustments for a probe or audit.
struct Walk {
    base_probation_density: f64,
    base_smoothed_rate: f64,
    base_request_count: u64,
    base_hit_rate: f64,
    base_window: u64,
    is_audit: bool,
    down: bool,
    below_bar_streak: i32,
    above_streak: i32,
    samples: i32,
    beat_base: bool,
    best_rate: f64,
    best_window: i64,
}

impl Walk {
    /// The scale shared by the audit's crash and reversal thresholds.
    const AUDIT_BAR_FRACTION: f64 = 0.15;
    /// Consecutive below-threshold samples required to abort an audit retry.
    const AUDIT_CRASH_PERSISTENCE: i32 = 3;
    /// Consecutive samples above the frozen reference that confirm an audit.
    const AUDIT_CONFIRM_STREAK: i32 = 4;
    /// The minimum samples before an audit can confirm.
    const AUDIT_COMMITMENT: i32 = 5;
    /// The hit count required for a density verdict, as a multiple of the bar.
    const PROBE_EXIT_BAR_MULTIPLE: u64 = 4;
    /// The starvation probe's maximum crash threshold.
    const PROBE_BAR_CAP: f64 = 3.0;
    /// The maximum samples before an unconfirmed walk fails.
    const PROBE_WALK_BUDGET: i32 = 16;

    fn new(
        is_audit: bool,
        down: bool,
        base_window: u64,
        base_request_count: u64,
        base_hit_rate: f64,
        base_smoothed_rate: f64,
        base_probation_density: f64,
    ) -> Self {
        Self {
            base_probation_density,
            base_smoothed_rate,
            base_request_count,
            base_hit_rate,
            base_window,
            is_audit,
            down,
            below_bar_streak: 0,
            above_streak: 0,
            samples: 0,
            beat_base: false,
            best_rate: -1.0,
            best_window: -1,
        }
    }

    fn remember_best(&mut self, above: bool, reading: &Reading) {
        if !above {
            self.best_window = -1;
            self.best_rate = -1.0;
        } else if reading.hit_rate >= self.best_rate {
            self.best_window = reading.window_max as i64;
            self.best_rate = reading.hit_rate;
        }
    }

    fn verdict_window(&self, reading: &Reading) -> u64 {
        let better = self.is_audit
            && (self.best_window >= 0)
            && (self.best_rate > (reading.hit_rate + VETO_MARGIN_MIN));
        if better {
            self.best_window as u64
        } else {
            reading.window_max
        }
    }

    fn direction(&self) -> f64 {
        if self.down {
            -1.0
        } else {
            1.0
        }
    }

    fn is_budget_spent(&self) -> bool {
        self.samples >= Self::PROBE_WALK_BUDGET
    }

    fn crosses_base(&self, position: f64) -> bool {
        if self.down {
            position > self.base_window as f64
        } else {
            position < self.base_window as f64
        }
    }

    fn crash_bar(&self, rates: &Rates) -> f64 {
        if self.is_audit {
            RESTART_THRESHOLD.min(Self::AUDIT_BAR_FRACTION * self.base_hit_rate)
        } else {
            (Self::PROBE_BAR_CAP * RESTART_THRESHOLD).min(RESTART_THRESHOLD.max(rates.noise_band()))
        }
    }

    fn reversal_bar(&self, rates: &Rates) -> f64 {
        if self.is_audit {
            RESTART_THRESHOLD
                .min(Self::AUDIT_BAR_FRACTION * self.base_hit_rate.max(rates.noise_band()))
        } else {
            self.crash_bar(rates)
        }
    }

    fn can_adjudicate(&self, reading: &Reading, commitment: i32) -> bool {
        let watched = if self.down {
            reading.main_hits
        } else {
            reading.window_hits
        };
        (watched >= (Self::PROBE_EXIT_BAR_MULTIPLE * reading.bar)) && (self.samples >= commitment)
    }

    fn verdict_signal(&self, reading: &Reading) -> f64 {
        if self.down {
            return reading.error();
        }
        let baseline = self.base_probation_density
            * (reading.request_count as f64 / self.base_request_count.max(1) as f64);
        ((reading.window_density + Reading::DENSITY_EPSILON)
            / (baseline + Reading::DENSITY_EPSILON))
            .ln()
    }

    fn should_crash_abort(&self, below_bar: bool, ladder_has_crashed: bool) -> bool {
        let tolerant = self.is_audit && ladder_has_crashed;
        below_bar && (!tolerant || (self.below_bar_streak >= Self::AUDIT_CRASH_PERSISTENCE))
    }

    fn is_confirmed(&self) -> bool {
        (self.samples >= Self::AUDIT_COMMITMENT)
            && (self.above_streak >= Self::AUDIT_CONFIRM_STREAK)
            && self.beat_base
    }

    fn is_reversed_by(&self, reading: &Reading) -> bool {
        (reading.steering_error() * self.direction()) < 0.0
    }

    fn is_audit_grade(&self, reading: &Reading) -> bool {
        (self.samples >= Ladder::PROBE_COMMITMENT_DEEP)
            && self.is_reversed_by(reading)
            && self.is_confirmed()
    }
}

/// Retry state for starvation probes or audits.
struct Ladder {
    farthest_down: bool,
    crash_streak: i32,
    farthest: i64,
    rung: i32,
}

impl Ladder {
    const PROBE_STRIDE_SCALE_MID: f64 = 2.0;
    const PROBE_STRIDE_SCALE_DEEP: f64 = 4.0;
    const PROBE_COMMITMENT_MID: i32 = 2;
    pub(crate) const PROBE_COMMITMENT_DEEP: i32 = 10;
    const PROBE_CRASH_ESCALATION: i32 = 2;
    const PROBE_BACKOFF_INITIAL: i32 = 16;
    const PROBE_BACKOFF_MAX: i32 = 64;

    fn new() -> Self {
        let mut ladder = Self {
            farthest_down: false,
            crash_streak: 0,
            farthest: -1,
            rung: 0,
        };
        ladder.reset();
        ladder
    }

    fn reset(&mut self) {
        self.rung = Self::PROBE_BACKOFF_INITIAL;
        self.crash_streak = 0;
        self.forget();
    }

    fn forget(&mut self) {
        self.farthest = -1;
    }

    fn is_repeat(&self, down: bool, window: u64, band: u64) -> bool {
        if (self.farthest < 0) || (down != self.farthest_down) {
            return false;
        }
        let farthest = self.farthest;
        let window = window as i64;
        let band = band as i64;
        if down {
            window >= (farthest - band)
        } else {
            window <= (farthest + band)
        }
    }

    fn remember(&mut self, down: bool, window: u64) {
        let window = window as i64;
        let farther = (self.farthest < 0)
            || (down != self.farthest_down)
            || (if down {
                window < self.farthest
            } else {
                window > self.farthest
            });
        if farther {
            self.farthest_down = down;
            self.farthest = window;
        }
    }

    fn escalate(&mut self) {
        self.rung = Self::PROBE_BACKOFF_MAX.min(2 * self.rung);
    }

    fn crash(&mut self) {
        self.crash_streak = Self::PROBE_CRASH_ESCALATION.min(self.crash_streak + 1);
    }

    fn reward(&mut self) {
        self.crash_streak = 0;
        self.rung = 1;
    }

    fn commitment_depth(&self) -> i32 {
        if self.rung >= Self::PROBE_BACKOFF_MAX {
            Self::PROBE_COMMITMENT_DEEP
        } else if self.rung >= (2 * Self::PROBE_BACKOFF_INITIAL) {
            Self::PROBE_COMMITMENT_MID
        } else {
            0
        }
    }

    fn stride(&self, reading: &Reading) -> f64 {
        reading
            .max_step()
            .min(self.stride_scale() * reading.restart_magnitude())
    }

    fn stride_scale(&self) -> f64 {
        if self.rung >= Self::PROBE_BACKOFF_MAX {
            Self::PROBE_STRIDE_SCALE_DEEP
        } else if self.rung >= (2 * Self::PROBE_BACKOFF_INITIAL) {
            Self::PROBE_STRIDE_SCALE_MID
        } else {
            1.0
        }
    }

    fn has_crashed(&self) -> bool {
        self.crash_streak >= 1
    }

    fn crash_escalates(&self) -> bool {
        self.crash_streak >= Self::PROBE_CRASH_ESCALATION
    }
}

/// The timing and direction of equilibrium audits.
struct AuditClock {
    settled_rate: f64,
    still_samples: i32,
    wait_samples: i32,
    last_window: i64,
    down: bool,
}

impl AuditClock {
    const AUDIT_WAIT_INITIAL: i32 = 32;
    const AUDIT_WAIT_FIRST: i32 = 4;
    const AUDIT_WAIT_MAX: i32 = 512;

    fn new() -> Self {
        let mut clock = Self {
            settled_rate: f64::NAN,
            still_samples: 0,
            wait_samples: 0,
            last_window: -1,
            down: true,
        };
        clock.reset();
        clock
    }

    fn reset(&mut self) {
        self.wait_samples = Self::AUDIT_WAIT_FIRST;
        self.settled_rate = f64::NAN;
        self.still_samples = 0;
        self.last_window = -1;
    }

    fn restart(&mut self) {
        self.still_samples = 0;
    }

    fn settle(&mut self, down: bool, rate: f64) {
        self.wait_samples = Self::AUDIT_WAIT_INITIAL;
        self.settled_rate = rate;
        self.down = down;
    }

    fn tick(&mut self, window_max: u64, band: u64) {
        let same_place =
            (self.last_window < 0) || ((window_max as i64 - self.last_window).abs() <= band as i64);
        self.still_samples = if same_place {
            (i32::MAX - 1).min(self.still_samples) + 1
        } else {
            0.max(self.still_samples - 1)
        };
        self.last_window = window_max as i64;
    }

    fn is_due(&self) -> bool {
        self.still_samples >= self.wait_samples
    }

    fn reschedule(&mut self, failed: bool, crashed: bool, rung: i32) {
        self.wait_samples = if failed && !crashed && (rung >= Ladder::PROBE_BACKOFF_MAX) {
            Self::AUDIT_WAIT_MAX.min(2 * self.wait_samples.max(rung))
        } else {
            Ladder::PROBE_BACKOFF_INITIAL.max(rung)
        };
    }

    fn choose_direction(
        &mut self,
        reading: &Reading,
        stride: f64,
        rate: f64,
        parked: bool,
    ) -> bool {
        if !self.settled_rate.is_nan() {
            if !parked || ((rate - self.settled_rate).abs() >= RESTART_THRESHOLD) {
                self.down = !self.down;
            }
            self.settled_rate = f64::NAN;
        }
        if (reading.window_max as f64) <= (2.0 * reading.floor) {
            return false;
        }
        if reading.window_max >= reading.upper_corner() {
            return true;
        }
        let room = if self.down {
            reading.window_max as f64 - reading.floor
        } else {
            reading.upper_corner() as f64 - reading.window_max as f64
        };
        if room < stride {
            self.down = !self.down;
        }
        let chosen = self.down;
        self.down = !chosen;
        chosen
    }
}

/// A reference position and hit rate used to reject sustained regressions.
struct Anchor {
    shortfall_streak: i32,
    retest_claim: f64,
    returning: bool,
    return_left: i32,
    settle_left: i32,
    fresh_left: i32,
    held: bool,
    window: i64,
    rate: f64,
}

impl Anchor {
    const VETO_RETURN_BUDGET: i32 = 8;
    const RETEST_SETTLE: i32 = 2;
    const VETO_STREAK: i32 = 4;

    fn new() -> Self {
        let mut anchor = Self {
            shortfall_streak: 0,
            retest_claim: -1.0,
            returning: false,
            return_left: 0,
            settle_left: 0,
            fresh_left: 0,
            held: false,
            window: -1,
            rate: 0.0,
        };
        anchor.reset();
        anchor
    }

    fn reset(&mut self) {
        self.shortfall_streak = 0;
        self.return_left = 0;
        self.end_return();
        self.discard();
    }

    fn discard(&mut self) {
        self.release();
        self.end_retest();
        self.window = -1;
    }

    fn is_planted(&self) -> bool {
        self.window >= 0
    }

    fn is_at(&self, window_max: u64, band: u64) -> bool {
        self.is_planted() && ((window_max as i64 - self.window).abs() <= band as i64)
    }

    fn is_away_from(&self, window_max: u64, band: u64) -> bool {
        self.is_planted() && !self.is_at(window_max, band)
    }

    fn plant(&mut self, window_max: u64, claimed: f64) {
        self.window = window_max as i64;
        self.rate = claimed;
        self.end_retest();
    }

    fn resync(&mut self, claimed: f64) {
        self.rate = claimed;
    }

    fn track(&mut self, reading: &Reading, rates: &Rates, probing: bool) {
        let settled = !probing && !self.returning;
        if !self.is_planted() {
            if settled {
                self.plant(reading.window_max, rates.smoothed);
            }
        } else if self.is_at(reading.window_max, reading.band) {
            self.resync(rates.smoothed);
        } else if settled && (rates.smoothed > (self.rate + rates.veto_margin())) {
            self.plant(reading.window_max, rates.smoothed);
            self.release();
        }
    }

    fn park(&mut self, shield: i32) {
        self.fresh_left = shield;
        self.held = true;
    }

    fn hold(&mut self) {
        self.held = true;
    }

    fn release(&mut self) {
        self.fresh_left = 0;
        self.held = false;
    }

    fn is_shielded(&self) -> bool {
        self.held && (self.fresh_left > 0)
    }

    fn age_shield(&mut self) {
        self.fresh_left -= 1;
    }

    fn veto_triggered(&mut self, reading: &Reading, rates: &Rates) -> bool {
        if self.is_away_from(reading.window_max, reading.band)
            && (rates.smoothed < (self.rate - rates.veto_margin()))
        {
            self.shortfall_streak += 1;
            if self.shortfall_streak >= Self::VETO_STREAK {
                self.shortfall_streak = 0;
                self.begin_return();
                return true;
            }
        } else {
            self.shortfall_streak = 0;
        }
        false
    }

    fn stand_down(&mut self, reading: &Reading) -> bool {
        let discarded = self.is_at(reading.window_max, reading.band);
        if discarded {
            self.discard();
        }
        self.release();
        self.end_return();
        self.shortfall_streak = 0;
        discarded
    }

    fn begin_return(&mut self) {
        self.return_left = Self::VETO_RETURN_BUDGET;
        self.settle_left = Self::RETEST_SETTLE;
        self.retest_claim = self.rate;
        self.returning = true;
        self.hold();
    }

    fn stride_home(&mut self, reading: &Reading) -> f64 {
        self.return_left -= 1;
        let remaining = self.window as f64 - reading.window_max as f64;
        if (remaining.abs() <= reading.max_step()) || (self.return_left <= 0) {
            self.end_return();
        }
        reading.capped_stride(remaining)
    }

    fn end_return(&mut self) {
        self.returning = false;
    }

    fn is_retest_due(&mut self, reading: &Reading) -> bool {
        if (self.retest_claim < 0.0) || self.returning {
            return false;
        }
        if !self.is_at(reading.window_max, reading.band) {
            self.end_retest();
            return false;
        }
        true
    }

    fn retest_fails(&mut self, rates: &Rates) -> bool {
        self.settle_left -= 1;
        if self.settle_left > 0 {
            return false;
        }
        let claimed = self.retest_claim;
        self.end_retest();
        rates.smoothed < (claimed - rates.veto_margin())
    }

    fn end_retest(&mut self) {
        self.retest_claim = -1.0;
        self.settle_left = 0;
    }
}

/// A smoothed hit rate and mean absolute deviation.
struct Rates {
    deviation: f64,
    smoothed: f64,
}

/// The shortfall margin as a multiple of the smoothed hit-rate deviation.
const VETO_MARGIN_SCALE: f64 = 3.0;
/// The floor on the guard rail's shortfall margin.
const VETO_MARGIN_MIN: f64 = 0.01;
/// The initial deviation estimate.
const DEVIATION_SEED: f64 = 0.05;
/// The smoothing constant for the goal-metric references.
const RATE_SMOOTHING: f64 = 0.2;

impl Rates {
    fn new() -> Self {
        Self {
            deviation: DEVIATION_SEED,
            smoothed: f64::NAN,
        }
    }

    fn reset(&mut self) {
        self.smoothed = f64::NAN;
        self.deviation = DEVIATION_SEED;
    }

    fn is_unseeded(&self) -> bool {
        self.smoothed.is_nan()
    }

    fn seed(&mut self, hit_rate: f64) {
        self.smoothed = hit_rate;
    }

    fn update(&mut self, hit_rate: f64) {
        self.deviation += RATE_SMOOTHING * ((hit_rate - self.smoothed).abs() - self.deviation);
        self.smoothed += RATE_SMOOTHING * (hit_rate - self.smoothed);
    }

    fn noise_band(&self) -> f64 {
        VETO_MARGIN_SCALE * self.deviation
    }

    fn veto_margin(&self) -> f64 {
        VETO_MARGIN_MIN.max(self.noise_band())
    }
}

/// The outcome of a walk sample.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ProbeEnding {
    Walking,
    Confirmed,
    Crashed,
    Failed,
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

    #[test]
    fn density_tier_starts_above_4096_entries() {
        assert!(matches!(WindowClimber::new(4096).tier, Tier::Reactive(_)));
        assert!(matches!(WindowClimber::new(4097).tier, Tier::Density(_)));
    }

    #[test]
    fn density_tier_grows_toward_the_window() {
        let mut climber = WindowClimber::new(10_000);
        for _ in 0..1_000 {
            climber.record_hit(true, false);
        }
        // The density tier wants four samples of the maximum, but the sketch
        // sample size caps the period.
        climber.determine_adjustment(10_000, 5_000, 4_000, 1_000);
        assert!(
            climber.adjustment() > 0,
            "window density dominates: {}",
            climber.adjustment()
        );
    }

    #[test]
    fn density_tier_shrinks_toward_main() {
        let mut climber = WindowClimber::new(10_000);
        for _ in 0..1_000 {
            climber.record_hit(false, true);
        }
        climber.determine_adjustment(10_000, 5_000, 4_000, 1_000);
        assert!(
            climber.adjustment() < 0,
            "main density dominates: {}",
            climber.adjustment()
        );
    }

    #[test]
    fn density_tier_probes_a_dead_window() {
        let mut climber = WindowClimber::new(10_000);
        for _ in 0..1_000 {
            climber.record_miss();
        }
        climber.determine_adjustment(10_000, 5_000, 4_000, 1_000);
        assert!(
            climber.adjustment() < 0,
            "a starved wide window is probed downward: {}",
            climber.adjustment()
        );
        let Tier::Density(density) = &climber.tier else {
            panic!("expected the density tier");
        };
        assert!(density.walk.is_some());
    }
}
