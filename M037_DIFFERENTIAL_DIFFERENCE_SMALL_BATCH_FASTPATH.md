# M037 Differential Difference Small-Batch Fast Path

This adds a small-batch fast path to `ExceptDifferentialRows`, the signed
differential implementation of `EXCEPT`. A single update, or a left/right pair
with the same key and logical time, is handled without the general keyed
consolidation map. All other inputs retain the existing implementation.

## Correctness

The fast path preserves:

- input-order payload and timestamp selection;
- signed multiplicity and exact cancellation;
- required-key validation;
- `math.MinInt64` negation errors;
- checked differential overflow errors;
- input immutability through row cloning;
- the existing general path for all other batch shapes.

Coverage is in
`hat/hatSql/m037_differential_difference_small_batch_test.go`.

## Focused Benchmark

Command: `make benchmark-codex-differential-difference`

Machine: AMD Ryzen 9 5950X, Linux amd64. Each version was sampled five times.

| Case | Version | ns/op samples | B/op | allocs/op |
| --- | --- | --- | ---: | ---: |
| Single left update | Before | 265.1, 253.2, 250.9, 259.1, 250.3 | 384 | 3 |
| Single left update | After | 209.2, 201.1, 200.4, 207.0, 202.4 | 384 | 3 |
| Exact cancellation | Before | 324.5, 325.8, 322.7, 327.9, 330.2 | 416 | 3 |
| Exact cancellation | After | 6.345, 5.930, 5.915, 6.018, 6.260 | 0 | 0 |

Median single-update cost improved from `253.2` to `202.4 ns/op`: `1.25x`
faster. Row cloning still determines its allocation footprint. Exact
cancellation improved from `325.8` to `6.018 ns/op`: `54.1x` faster, with
`416` to `0 B/op` and `3` to `0 allocs/op`.

## Large-Batch Control

Command: `make benchmark-codex-differential-difference-full`

The existing 4,096-row control does not enter the new branch. Its post-change
samples were:

| Case | ns/op samples | B/op samples | allocs/op samples |
| --- | --- | --- | --- |
| Baseline composition | 2492695, 2514610, 2355563, 2316742, 2374387 | 4752034, 4752021, 4752022, 4752019, 4752022 | 20515, 20515, 20515, 20515, 20515 |
| Optimized existing path | 892468, 887728, 874175, 883604, 902530 | 1671816, 1671815, 1671815, 1671814, 1671815 | 4130, 4130, 4130, 4130, 4130 |

## Verification

Focused tests, race tests, focused benchmarks, and the full `hat/hatSql`
package test were run through temporary Makefile-backed scripts with isolated
cache directories; all passed. The temporary helpers were removed after the
measurements, while the benchmark functions remain in the test file for future
targeted runs.
