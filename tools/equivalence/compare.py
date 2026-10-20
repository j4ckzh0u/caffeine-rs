#!/usr/bin/env python3
"""Compares the Java Caffeine and Rust caffeine-rs workload results."""

import sys

MAX_HIT_RATE_DELTA = 0.02
MAX_EVICTION_DELTA = 0.10


def parse(path):
    with open(path, encoding="utf-8") as handle:
        parts = handle.read().split()
    if len(parts) != 3:
        raise SystemExit(f"{path}: expected 3 integers, got {parts!r}")
    return tuple(int(part) for part in parts)


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: compare.py <java-output> <rust-output>")

    java_hits, java_misses, java_evictions = parse(sys.argv[1])
    rust_hits, rust_misses, rust_evictions = parse(sys.argv[2])

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
        "java: hits={} misses={} evictions={} hit_rate={:.4f}".format(
            java_hits, java_misses, java_evictions, java_rate
        )
    )
    print(
        "rust: hits={} misses={} evictions={} hit_rate={:.4f}".format(
            rust_hits, rust_misses, rust_evictions, rust_rate
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
