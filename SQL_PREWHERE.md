# SQL PREWHERE

`PREWHERE` is a ClickHouse-inspired optional early filter for selective SQL
reads. It is evaluated before the ordinary `WHERE` predicate and before
projected result rows are materialized.

```sql
FROM CACHE('items') AS item
PREWHERE item.region = 'apac'
WHERE item.payload LIKE '%needle%' AND item.score >= 900
SELECT item.id, item.payload
```

The clause preserves SQL filtering behavior: `NULL` and `FALSE` reject a row,
and the late `WHERE` is evaluated only after `PREWHERE` succeeds. Parameters
are supported through the normal `$1`, `$2`, ... query API.

The separate streaming stage is selected only for an untyped, single-source
`CACHE` query with a `StreamSourceResolver`, no joins, grouping, ordering,
sampling, subqueries, or custom functions. It evaluates the early predicate,
then the late predicate, and materializes only matching projections. Existing
ordinary `WHERE` queries keep their current behavior.

Resolvers that provide index, columnar, or ordered execution are deliberately
given a combined logical predicate instead. This keeps existing physical plans
correct until they grow an explicit two-stage contract. The materialized
fallback therefore has the same result semantics and a small query-wrapper
cost, while the stream path carries the measured benefit.

## Benchmark

Run `make benchmark-ch001-prewhere`. The benchmark uses 20,000 rows, a cheap
boolean `PREWHERE` that keeps one in sixteen rows, and an expensive string
`WHERE` predicate that otherwise runs first. It reports five Go benchmark
samples with time, heap, and allocation counts.

On Linux with an AMD Ryzen 9 5950X, the current medians are:

| Path | Time (ns/op) | Heap (B/op) | Allocations (allocs/op) |
| --- | ---: | ---: | ---: |
| Ordinary `WHERE`, stream resolver | 11,284,432 | 2,793,475 | 100,769 |
| Explicit `PREWHERE`, stream resolver | 3,822,869 | 493,028 | 44,034 |
| Ordinary `WHERE`, materialized resolver | 12,906,560 | 2,961,665 | 100,845 |
| Explicit `PREWHERE`, materialized fallback | 12,932,641 | 3,041,910 | 101,673 |

Compared with the ordinary stream path, explicit `PREWHERE` is 2.95x faster,
uses 5.67x less heap, and performs 2.29x fewer allocations. The materialized
fallback is within benchmark noise for latency, with a 2.7% heap and 0.8%
allocation increase from preserving the explicit two-stage syntax.

The benchmark is in-process and excludes network transport, serialization, and
storage I/O.
