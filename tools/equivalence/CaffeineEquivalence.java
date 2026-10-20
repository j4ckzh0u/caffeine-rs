import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

/**
 * Replays the deterministic workload used by
 * {@code examples/equivalence_trace.rs} through Java Caffeine.
 *
 * <p>Output: {@code hits misses evictions} separated by spaces.
 */
public final class CaffeineEquivalence {
  private static final int CAPACITY = 2_000;
  private static final int ACCESSES = 200_000;

  public static void main(String[] args) {
    Cache<Long, Long> cache = Caffeine.newBuilder()
        .maximumSize(CAPACITY)
        .recordStats()
        .build();

    long state = 0x9E3779B97F4A7C15L;
    for (int i = 0; i < ACCESSES; i++) {
      state = (state * 6364136223846793005L) + 1442695040888963407L;
      long key = ((state >>> 33) % 100 < 80)
          ? (state >>> 17) % 2_000
          : (state >>> 17) % 20_000;
      cache.get(key, k -> k);
    }

    CacheStats stats = cache.stats();
    System.out.printf("%d %d %d%n",
        stats.hitCount(), stats.missCount(), stats.evictionCount());
  }
}
