# M038 Differential Union Small-Batch Fast Path

This adds a bounded scalar consolidation path to `UnionDifferentialRows` for a
single update or two rows with the same key and logical time. It preserves the
general keyed consolidation path for larger or multi-identity batches.

## Correctness

The fast path preserves:

- first-seen payload and timestamp ordering;
- signed multiplicity and exact cancellation;
- required-key validation;
- checked overflow fallback to the existing error path;
- zero-diff behavior;
- input immutability through row cloning;
- the existing general path for all other batches.

Coverage is in `hat/hatSql/m038_differential_union_small_batch_test.go`.

## Benchmark

Command: `make benchmark-codex-differential-union`

Machine: AMD Ryzen 9 5950X, Linux amd64. Each version was sampled five times.

| Case | Version | ns/op samples | B/op | allocs/op |
| --- | --- | --- | ---: | ---: |
| Single update | Before | 299.8, 292.2, 289.4, 287.4, 288.9 | 432 | 4 |
| Single update | After | 213.5, 205.1, 204.6, 202.8, 202.3 | 384 | 3 |
| Exact cancellation | Before | 549.6, 546.9, 546.2, 554.8, 551.3 | 832 | 6 |
| Exact cancellation | After | 17.56, 17.56, 17.57, 17.60, 17.54 | 0 | 0 |

The single-update median improved from `289.4` to `204.6 ns/op`: `1.41x`
faster, `11.1%` fewer bytes, and `25%` fewer allocations. Exact cancellation
improved from `549.6` to `17.56 ns/op`: `31.3x` faster, with `832` to `0 B/op`
and `6` to `0 allocs/op`.

## Verification

Focused tests, race tests, focused benchmarks, and the full `hat/hatSql`
package test passed through temporary Makefile-backed scripts using isolated
cache directories. The temporary helpers were removed after measurement; the
benchmark function remains in the test file.
