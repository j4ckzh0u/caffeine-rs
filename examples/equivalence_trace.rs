//! Replays the deterministic workload used by the Java Caffeine harness in
//! `tools/equivalence/CaffeineEquivalence.java`.
//!
//! Output: `hits misses evictions` separated by spaces.

use caffeine::Cache;

const CAPACITY: usize = 2_000;
const ACCESSES: usize = 200_000;

fn main() {
    let cache: Cache<u64, u64> = Cache::builder()
        .maximum_size(CAPACITY)
        .record_stats(true)
        .build();

    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..ACCESSES {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let key = if ((state >> 33) % 100) < 80 {
            (state >> 17) % 2_000
        } else {
            (state >> 17) % 20_000
        };
        cache.get_with(key, |key| *key);
    }

    let stats = cache.stats();
    println!(
        "{} {} {}",
        stats.hit_count, stats.miss_count, stats.eviction_count
    );
}
