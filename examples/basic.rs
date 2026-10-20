use std::time::Duration;

use caffeine::Cache;

fn main() {
    let cache: Cache<String, u64> = Cache::builder()
        .maximum_size(1_000)
        .expire_after_write(Duration::from_secs(60))
        .record_stats(true)
        .build();

    for i in 0..100 {
        cache.insert(format!("key-{i}"), i);
    }

    let answer = cache.get_with("answer".to_string(), |_| 42);
    let stats = cache.stats();

    println!(
        "answer={answer} entries={} hit_rate={:.2}",
        cache.len(),
        stats.hit_rate()
    );
    println!("policy maximum={}", cache.policy().maximum());
}
