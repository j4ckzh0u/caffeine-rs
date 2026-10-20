#!/usr/bin/env python3
"""Compares the Java Caffeine and Rust caffeine-rs workload results.

Both harnesses emit one `hits misses evictions` line per trial with a fresh
cache/hash seed. Comparing aggregate rates over multiple trials removes the
run-to-run variance caused by randomized frequency-sketch hashing.
"""

import sys

MAX_HIT_RATE_DELTA = 0.02
MAX_EVICTION_DELTA = 0.10


def parse(path):
    with open(path, encoding="utf-8") as handle:
        lines = [line.split() for line in handle if line.strip()]
    if not lines:
        raise SystemExit(f"{path}: no trial results")
    trials = []
    for parts in lines:
        if len(parts) != 3:
            raise SystemExit(f"{path}: expected 3 integers, got {parts!r}")
        trials.append(tuple(int(part) for part in parts))
    return trials


def aggregate(trials):
    return tuple(sum(values) for values in zip(*trials))


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: compare.py <java-output> <rust-output>")

    java_trials = parse(sys.argv[1])
    rust_trials = parse(sys.argv[2])
    if len(java_trials) != len(rust_trials):
        raise SystemExit(
            f"trial count mismatch: java={len(java_trials)} rust={len(rust_trials)}"
        )

    java_hits, java_misses, java_evictions = aggregate(java_trials)
    rust_hits, rust_misses, rust_evictions = aggregate(rust_trials)

    java_total = java_hits + java_misses
    rust_total = rust_hits + rust_misses
    if java_total != rust_total:
        raise SystemExit(
            f"request count mismatch: java={java_total} rust={rust_total}"
        )

    java_rate = java_hits / java_total
    rust_rate = rust_hits / rust_total
    rate_delta = abs(java_rate - rust_rate)
    eviction_delta = abs(java_evictions - rust_evictions) / max(
        java_evictions, rust_evictions, 1
    )

    print(
        "java: trials={} hits={} misses={} evictions={} hit_rate={:.4f}".format(
            len(java_trials), java_hits, java_misses, java_evictions, java_rate
        )
    )
    print(
        "rust: trials={} hits={} misses={} evictions={} hit_rate={:.4f}".format(
            len(rust_trials), rust_hits, rust_misses, rust_evictions, rust_rate
        )
    )

    failures = []
    if rate_delta > MAX_HIT_RATE_DELTA:
        failures.append(
            f"hit-rate delta {rate_delta:.4f} exceeds {MAX_HIT_RATE_DELTA:.4f}"
        )
    if eviction_delta > MAX_EVICTION_DELTA:
        failures.append(
            f"eviction delta {eviction_delta:.4f} exceeds {MAX_EVICTION_DELTA:.4f}"
        )
    if failures:
        raise SystemExit("equivalence check failed: " + "; ".join(failures))

    print(
        "equivalence passed: hit-rate delta={:.4f}, eviction delta={:.4f}".format(
            rate_delta, eviction_delta
        )
    )


if __name__ == "__main__":
    main()
