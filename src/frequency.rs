//! A 4-bit Count-Min sketch backing the TinyLFU admission policy.

const DEPTH: usize = 4;
const SEEDS: [u64; DEPTH] = [
    0xc3a5_c85c_97cb_3127,
    0xb492_b66f_be98_f273,
    0x9ae1_6a3b_2f90_404f,
    0xcbf2_9ce4_8422_2325,
];

/// Counters are 4 bits wide, so a `u64` word holds 16 of them.
const COUNTERS_PER_WORD: usize = 16;
const HALVE_MASK: u64 = 0x7777_7777_7777_7777;
const MAX_WIDTH: u64 = 1 << 20;

/// Approximate frequency counts with 4-bit saturating counters.
/// The sketch ages by halving every counter once it has seen ten times the
/// cache capacity, keeping recent popularity dominant.
pub(crate) struct FrequencySketch {
    table: Vec<u64>,
    width: usize,
    mask: usize,
    sample_size: u64,
    size: u64,
}

impl FrequencySketch {
    pub(crate) fn new(capacity: u64) -> Self {
        let width = capacity.clamp(1, MAX_WIDTH) as usize;
        let width = width.next_power_of_two();
        let words = (width * DEPTH / COUNTERS_PER_WORD).max(1);
        Self {
            table: vec![0; words],
            width,
            mask: width - 1,
            sample_size: capacity.saturating_mul(10).max(10),
            size: 0,
        }
    }

    /// Estimates the frequency of `hash`, between 0 and 15.
    pub(crate) fn frequency(&self, hash: u64) -> u8 {
        let mut min = u8::MAX;
        for (row, seed) in SEEDS.iter().enumerate() {
            let index = self.index(hash, *seed, row);
            min = min.min(self.counter(index));
        }
        min
    }

    /// Records one usage of `hash`, aging the sketch when it is due.
    pub(crate) fn increment(&mut self, hash: u64) {
        let mut added = false;
        for (row, seed) in SEEDS.iter().enumerate() {
            let index = self.index(hash, *seed, row);
            added |= self.increment_at(index);
        }
        if added {
            self.size += 1;
            if self.size >= self.sample_size {
                self.reset();
            }
        }
    }

    fn index(&self, hash: u64, seed: u64, row: usize) -> usize {
        let mixed = mix(hash.wrapping_add(seed));
        row * self.width + (mixed as usize & self.mask)
    }

    fn counter(&self, index: usize) -> u8 {
        let word = self.table[index / COUNTERS_PER_WORD];
        ((word >> ((index % COUNTERS_PER_WORD) * 4)) & 0xf) as u8
    }

    fn increment_at(&mut self, index: usize) -> bool {
        let word = &mut self.table[index / COUNTERS_PER_WORD];
        let shift = (index % COUNTERS_PER_WORD) * 4;
        let mask = 0xfu64 << shift;
        if *word & mask == mask {
            false
        } else {
            *word += 1u64 << shift;
            true
        }
    }

    fn reset(&mut self) {
        for word in &mut self.table {
            *word = (*word >> 1) & HALVE_MASK;
        }
        self.size /= 2;
    }
}

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frequency_saturates_and_ages() {
        // A large capacity delays aging long enough to observe saturation.
        let mut sketch = FrequencySketch::new(1_000);
        for _ in 0..100 {
            sketch.increment(42);
        }
        assert_eq!(sketch.frequency(42), 15);
        assert_eq!(sketch.frequency(7), 0);

        // A tiny capacity ages aggressively: ten increments trigger a halving.
        let mut aged = FrequencySketch::new(1);
        for _ in 0..10 {
            aged.increment(42);
        }
        assert!(aged.frequency(42) < 10);
    }

    #[test]
    fn sketch_is_deterministic() {
        let mut a = FrequencySketch::new(128);
        let mut b = FrequencySketch::new(128);
        for key in 0..500 {
            a.increment(key);
            b.increment(key);
        }
        for key in 0..500 {
            assert_eq!(a.frequency(key), b.frequency(key));
        }
    }
}
