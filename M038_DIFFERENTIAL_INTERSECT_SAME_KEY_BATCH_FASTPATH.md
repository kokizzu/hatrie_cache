# M038 Differential Intersect Same-Key Batch Fast Path

This adds a bounded scalar validation path to `DifferentialIntersect.Apply` for
batches of up to eight rows when every row targets the same key. It avoids the
temporary keyed validation map and lazily avoids allocating an output slice
when the batch produces no transitions. Multi-key and larger batches retain
the existing staged map-backed path.

## Correctness

The fast path preserves the general path's behavior:

- all updates are validated before the operator is mutated;
- negative multiplicity and output-diff overflow errors remain atomic;
- left payload retention and right-side output payloads are unchanged;
- updates are applied in left-then-right order;
- zero-diff rows remain no-ops;
- multi-key and batches larger than eight rows use the existing fallback.

Regression coverage is in
`hat/hatSql/m038_differential_intersect_same_key_batch_test.go`, including an
invalid-batch atomicity check. The benchmark is in
`hat/hatSql/m038_differential_intersect_same_key_batch_benchmark_test.go`.

## Targeted Benchmark

Command: `make benchmark-codex-m038-same-key`

Machine: AMD Ryzen 9 5950X, Linux amd64. Each version was sampled five times.

| Version | ns/op samples | B/op samples | allocs/op samples |
| --- | --- | ---: | ---: |
| Before | 143.4, 139.1, 139.8, 139.9, 139.0 | 80, 80, 80, 80, 80 | 1, 1, 1, 1, 1 |
| After | 70.31, 69.77, 65.92, 65.29, 66.29 | 0, 0, 0, 0, 0 | 0, 0, 0, 0, 0 |

The median improved from `139.8` to `66.29 ns/op`: `2.11x` faster, `52.6%`
less CPU time, and zero bytes/allocations for this no-transition batch.

## Control Benchmark

Command: `make benchmark-differential-intersect`

The existing single-update and full-rebuild controls remain on their original
paths. Incremental control samples before/after were:

| Version | ns/op samples | B/op | allocs/op |
| --- | --- | ---: | ---: |
| Before | 375417, 385698, 377184, 374113, 376595 | 651388 | 2709 |
| After | 388701, 380249, 376649, 377074, 379229 | 651388 | 2709 |

The control median is within normal benchmark noise; the optimized branch is
only selected for batches with more than one row and one shared key.

## Verification

- `make test-differential-intersect`
- `make verify-differential-intersect`
- `make benchmark-codex-m038-same-key`
- `make benchmark-differential-intersect`
