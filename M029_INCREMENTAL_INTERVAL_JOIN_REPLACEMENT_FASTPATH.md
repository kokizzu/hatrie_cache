# M029 Incremental Interval-Join Replacement Fast Path

## Adopted idea

Materialize-style differential maintenance treats a delete followed by an
insert for the same source key as one local replacement transition. Tarantool
also favors updating an existing tuple in place when its identity is stable.
The interval join now applies that exact two-update shape without creating the
two pending maps used by the general batch path.

The fast path is selected only when both updates:

- target the same valid side and source key;
- are an existing-key negative update followed by a positive update; and
- pass the same row, key, interval, multiplicity, overflow, and merge checks
  as the general path.

All other batches continue through the existing generic implementation.

## Measurement

Workload: `BenchmarkMZ029IncrementalIntervalJoinIncremental`, five benchmark
samples, AMD Ryzen 9 5950X, `-benchmem`.

| Version | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Before fast path | 1,736 | 2,048 | 20 |
| After fast path | 1,407 | 1,808 | 15 |
| Change | 1.23x faster | 11.7% lower | 25.0% lower |

The benchmark exercises a replacement of one retained left interval while a
10,000-row interval join is already populated. The output remains a negative
delta for the old row followed by a positive delta for the replacement.

## Verification

- Red regression test first failed because `applyReplacement` did not exist.
- Focused replacement test passed after implementation.
- All interval-join tests passed.
- Full `hat/hatSql` package tests passed.
- Race-selected interval-join tests passed.
- Temporary benchmark/test directories use traps and were removed after each
  run; the repository temporary-directory audit found no stale candidates.

## Tradeoffs

The implementation adds a specialized branch and helper code, but it does not
change retained data structures or behavior outside the exact replacement
shape. It still allocates match slices and output rows, so it is deliberately
bounded rather than attempting a risky zero-allocation rewrite.
