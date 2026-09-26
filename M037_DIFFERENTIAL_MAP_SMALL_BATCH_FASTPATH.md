# M037 Differential Map Small-Batch Fast Path

## Change

`MapDifferentialRows` now handles exactly one input row without building an
intermediate slice or running the general consolidation pass. The callback
still receives a private row clone, and the returned row is cloned before it
is exposed. Zero-weight rows, invalid keys, callback errors, duplicate rows,
and batches larger than one row retain the existing behavior.

## Benchmark

Machine: AMD Ryzen 9 5950X 16-Core Processor, Linux amd64.

Each case was run five times by the repository benchmark target. The table
uses the median of the five samples; raw samples are retained below.

| Case | Before | After | Improvement |
| --- | ---: | ---: | ---: |
| single mapped row, ns/op | 608.3 | 519.2 | 1.17x faster |
| single mapped row, B/op | 1,104 | 1,056 | 1.05x lower |
| single mapped row, allocs/op | 8 | 7 | 1 fewer allocation |
| single zero-diff row, ns/op | 30.63 | 4.22 | 7.26x faster |
| single zero-diff row, B/op | 48 | 0 | 48 B eliminated |
| single zero-diff row, allocs/op | 1 | 0 | 1 allocation eliminated |

### Raw samples

| Case | Before samples | After samples |
| --- | --- | --- |
| single mapped row, ns/op | 634.3, 602.9, 614.7, 600.2, 608.3 | 529.2, 522.5, 519.2, 516.5, 508.2 |
| single zero-diff row, ns/op | 30.29, 30.52, 30.66, 30.83, 30.63 | 4.219, 4.220, 4.214, 4.214, 4.233 |

The before and after runs used the same benchmark cases and five-sample
configuration. The optimization is deliberately limited to the one-row path;
multi-row mapping still uses consolidation and is not represented by this
microbenchmark.

## Verification

- Focused correctness tests passed before the implementation change.
- Focused correctness tests passed after the implementation change.
- `go test -race ./hat/hatSql` passed.
- Full `go test ./hat/hatSql` passed.
- Tests cover signed weights, zero weights, callback errors, input ownership,
  output ownership, and duplicate-key consolidation fallback.
