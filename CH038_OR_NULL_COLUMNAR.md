# CH-038 OrNull Columnar Aggregates

This change adopts the ClickHouse-style aggregate combinator behavior for the
native columnar grouped-query path. `COUNT_OR_NULL`, `SUM_OR_NULL`,
`AVG_OR_NULL`, `MIN_OR_NULL`, and `MAX_OR_NULL` are normalized to their base
aggregate kernels while retaining an explicit `orNull` flag for the result
contract.

## Semantics

- `COUNT_OR_NULL(*)` counts rows in a group and returns `NULL` only when the
  aggregate saw no rows.
- `COUNT_OR_NULL(column)` counts non-null values and returns `NULL` when none
  were present.
- Numeric `*_OR_NULL` aggregates return `NULL` when no non-null numeric value
  was observed; otherwise they return the same value as the corresponding base
  aggregate.
- The normal grouped row evaluator remains available for unsupported query
  shapes and source resolvers that do not provide columnar batches.

The columnar plan still uses the existing bounded projection and grouping
contracts. The change only broadens aggregate admission; it does not change
source ownership, null coercion, or two-level grouping thresholds.

## Verification

Focused correctness test:

```text
make test-ch038-or-null-columnar
```

The test verifies plan admission, requested columns, ordinary values, and
all-null groups for every supported `*_OR_NULL` aggregate.

## Benchmark

Command:

```text
make benchmark-ch038-or-null-columnar
```

The workload groups 4,096 rows into 32 groups and evaluates five nullable
aggregates. The pre-change run is the original general evaluator captured
before the plan admission change. The paired fallback and fast-path runs are
post-change runs over the same logical workload.

| Path | Median ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | ---: |
| Pre-change general evaluator | 3,676,138 | 5,762,310 | 37,360 | baseline |
| Paired row fallback | 2,204,419 | 2,067,728 | 13,032 | 1.00x |
| Columnar OrNull fast path | 790,218 | 195,517 | 4,783 | 2.79x faster than fallback; 4.65x faster than pre-change |

Relative to the pre-change run, the fast path retains about 29.5x less heap
per operation and performs about 7.8x fewer allocations. Relative to the
paired fallback it retains about 10.6x less heap and performs about 2.7x fewer
allocations.

Raw five-sample output after the change:

```text
BenchmarkCH038OrNullColumnarGroupBaseline-32  1430  812994 ns/op  195520 B/op  4783 allocs/op
BenchmarkCH038OrNullColumnarGroupBaseline-32  1480  797880 ns/op  195526 B/op  4783 allocs/op
BenchmarkCH038OrNullColumnarGroupBaseline-32  1470  795375 ns/op  195521 B/op  4783 allocs/op
BenchmarkCH038OrNullColumnarGroupBaseline-32  1441  799308 ns/op  195513 B/op  4783 allocs/op
BenchmarkCH038OrNullColumnarGroupBaseline-32  1508  801863 ns/op  195515 B/op  4783 allocs/op
BenchmarkCH038OrNullColumnarGroupFallback-32   556  2152229 ns/op 2067712 B/op 13032 allocs/op
BenchmarkCH038OrNullColumnarGroupFallback-32   546  2204419 ns/op 2067745 B/op 13032 allocs/op
BenchmarkCH038OrNullColumnarGroupFallback-32   538  2238053 ns/op 2067780 B/op 13032 allocs/op
BenchmarkCH038OrNullColumnarGroupFallback-32   546  2184054 ns/op 2067728 B/op 13032 allocs/op
BenchmarkCH038OrNullColumnarGroupFallback-32   548  2205652 ns/op 2067725 B/op 13032 allocs/op
BenchmarkCH038OrNullColumnarGroupFastPath-32  1461   790218 ns/op  195508 B/op  4783 allocs/op
BenchmarkCH038OrNullColumnarGroupFastPath-32  1503   787715 ns/op  195519 B/op  4783 allocs/op
BenchmarkCH038OrNullColumnarGroupFastPath-32  1479   789782 ns/op  195517 B/op  4783 allocs/op
BenchmarkCH038OrNullColumnarGroupFastPath-32  1464   812410 ns/op  195513 B/op  4783 allocs/op
BenchmarkCH038OrNullColumnarGroupFastPath-32  1480   795808 ns/op  195511 B/op  4783 allocs/op
```
