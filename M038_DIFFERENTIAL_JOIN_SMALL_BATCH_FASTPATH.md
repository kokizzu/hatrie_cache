# M038 Differential Join Small-Batch Fast Path

`JoinDifferentialRows` now uses a bounded scalar path when both inputs contain
exactly one row. It validates keys and zero weights, clones callback inputs,
preserves signed weight multiplication and overflow errors, and clones the
projected output. Larger inputs retain the existing consolidation and nested
join path.

## Correctness

Coverage is in `hat/hatSql/m038_differential_join_small_batch_test.go`:

- signed multiplicity and latest timestamp;
- callback input ownership;
- empty and zero-weight inputs;
- callback error propagation.

The existing differential operator tests also cover invalid keys, output
errors, and weight overflow.

## Benchmark

Command: `make benchmark-codex-m038-join`

Machine: AMD Ryzen 9 5950X, Linux amd64; five samples per case.

| Case | Before ns/op | After ns/op | Speedup | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| single match | 1104, 1043, 1022, 1032, 1017 | 800.4, 764.6, 754.3, 748.4, 748.3 | 1.37x | 1536 | 1392 | 12 | 9 |
| single non-match | 542.8, 538.6, 540.8, 538.3, 539.2 | 377.1, 370.8, 380.9, 390.1, 374.4 | 1.43x | 768 | 672 | 6 | 4 |

Median comparison: matching joins improved from `1032 ns/op` to `754.3 ns/op`;
nonmatching joins improved from `539.2 ns/op` to `377.1 ns/op`. The fallback
for multi-row batches is unchanged.

## Verification

Focused correctness tests, race tests, and the full `hat/hatSql` package pass.
Temporary benchmark/test caches are isolated under `/tmp` and removed by each
runner's exit trap.
