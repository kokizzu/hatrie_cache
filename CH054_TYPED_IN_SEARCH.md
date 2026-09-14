# CH-054 Typed Prepared `IN` Search

This is the second ClickHouse-style prepared-set improvement. Large enough
homogeneous literal `IN` lists are sorted in place at bind time and searched
with a typed binary search at evaluation time. Small lists keep the compact
linear scan because their scan is cheaper than search setup.

## Scope

- Lists with at least eight non-`NULL` numeric literals use numeric search.
- Lists with at least eight non-`NULL` string literals use binary-text search.
- Numeric comparison still follows the existing cross-type `sqlNumber` rules.
- `NULL`, mixed-type, dynamic, and non-binary-collation cases use the existing
  linear prepared-set comparator.
- The existing value slice is sorted in place; no second typed backing array
  or global cache is retained.
- Bind-time sorting is the only added preparation work, and storage/wire
  formats are unchanged.

## Measurement

Samples were collected on Linux `amd64`, Go benchmark workers `-32`, an AMD
Ryzen 9 5950X, and five runs per case. The baseline is the CH-053 prepared
literal slice; the optimized case adds typed binary search.

| Workload | Before ns/op | After ns/op | CPU improvement | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 numeric literals, hit | 151.9 | 40.12 | 3.79x | 0 | 0 | 0 | 0 |
| 32 numeric literals, hit | 483.9 | 41.46 | 11.67x | 0 | 0 | 0 | 0 |
| 32 numeric literals, miss | 498.9 | 44.68 | 11.17x | 0 | 0 | 0 | 0 |
| 8 string literals, hit | 156.1 | 48.15 | 3.24x | 0 | 0 | 0 | 0 |

The threshold is deliberately conservative: lists below eight values remain
linear and all fallback cases retain their prior behavior. The tradeoff is a
one-time in-place sort during binding; the measured evaluation path has no
additional heap or allocation cost.

## Raw output

```text
# Before, CH-053 prepared linear scan
BenchmarkCH054InSearchEvaluation/numeric_8_hit-32  7838455  151.9 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_8_hit-32  7948450  148.5 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_8_hit-32  8583564  144.2 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_8_hit-32  7624726  155.2 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_8_hit-32  7875898  152.3 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_hit-32  2448313  483.9 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_hit-32  2340613  466.7 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_hit-32  2746899  478.4 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_hit-32  2507314  511.9 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_hit-32  2245411  525.4 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_miss-32  2113420  554.4 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_miss-32  2207139  473.4 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_miss-32  2288974  498.9 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_miss-32  2580309  554.1 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_miss-32  2279275  487.4 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/string_8_hit-32  6947762  157.5 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/string_8_hit-32  7326661  156.1 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/string_8_hit-32  7818128  166.0 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/string_8_hit-32  7805478  154.3 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/string_8_hit-32  8106170  149.4 ns/op  0 B/op  0 allocs/op

# After, CH-054 typed binary search
BenchmarkCH054InSearchEvaluation/numeric_8_hit-32  29596399  40.12 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_8_hit-32  30256676  42.33 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_8_hit-32  22543302  46.92 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_8_hit-32  29418082  36.99 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_8_hit-32  26447077  40.47 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_hit-32  29293639  41.46 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_hit-32  28571700  39.85 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_hit-32  30425050  39.88 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_hit-32  27340485  41.52 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_hit-32  30512667  42.72 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_miss-32  27088730  43.03 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_miss-32  26015700  44.68 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_miss-32  26402178  44.51 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_miss-32  26867592  45.06 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/numeric_32_miss-32  25466389  45.43 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/string_8_hit-32  21014980  54.95 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/string_8_hit-32  25266268  48.15 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/string_8_hit-32  26127102  46.99 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/string_8_hit-32  23786100  47.75 ns/op  0 B/op  0 allocs/op
BenchmarkCH054InSearchEvaluation/string_8_hit-32  23522499  48.25 ns/op  0 B/op  0 allocs/op
```

## Verification

```text
make test-ch054-in-search
make benchmark-ch054-in-search
make format-ch054-in-search
make race-ch054-in-search
make vet-ch054-in-search
```
