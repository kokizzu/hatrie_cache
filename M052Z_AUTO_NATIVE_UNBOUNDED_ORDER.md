# M052z Automatic Native Unbounded ORDER BY

M052z extends the ClickHouse-style native dataflow selector to plain scalar
queries with `ORDER BY` and no finite `LIMIT`. The existing native ordered
operator already keeps the best rows in a bounded heap for Top-N queries; for
an unbounded result its capacity is the validated input row count, so the same
operator produces a complete stable ordering without changing SQL semantics.

## Eligibility

The automatic path is selected only when all of these remain true:

- the source is an ordinary `CACHE` or `KEYS` row resolver;
- projections and order expressions are direct scalar fields;
- there are no joins, CTEs, unions, aggregates, windows, grouping, distinct,
  `LIMIT BY`, `WITH TIES`, or `WITH FILL` clauses;
- no specialized resolver contract or planner option is requested.

`DisableNativeDataflow` still forces the established executor. A finite
`LIMIT` keeps the existing Top-N behavior, while an unlimited query retains
all input candidates and applies `OFFSET` after ordering. `LIMIT 0` and
unsupported shapes are unchanged.

## Measurement

The deterministic fixture has 4,096 rows, one `WHERE` predicate, two selected
fields, and descending order. Five `-benchmem` samples were collected on
Linux/amd64 with an AMD Ryzen 9 5950X.

| Path | ns/op samples | B/op samples | allocs/op samples |
| --- | --- | --- | --- |
| Before, automatic fallback | 7,531,478; 7,876,480; 7,735,440; 7,380,924; 7,549,009 | 3,807,879; 3,807,841; 3,807,844; 3,807,842; 3,807,841 | 20,516; 20,516; 20,516; 20,516; 20,516 |
| After, forced fallback control | 7,670,577; 7,885,932; 8,067,010; 8,275,319; 8,344,872 | 3,807,882; 3,807,879; 3,807,842; 3,807,841; 3,807,884 | 20,516; 20,516; 20,516; 20,516; 20,516 |
| After, automatic native full order | 6,200,514; 6,031,425; 5,930,780; 5,960,180; 5,792,144 | 2,071,840; 2,071,843; 2,071,841; 2,071,839; 2,071,840 | 12,319; 12,319; 12,319; 12,319; 12,319 |

| Comparison | Median result |
| --- | ---: |
| Before automatic fallback | 7,549,009 ns/op; 3,807,842 B/op; 20,516 allocs/op |
| After forced fallback control | 8,067,010 ns/op; 3,807,879 B/op; 20,516 allocs/op |
| After automatic native full order | 5,960,180 ns/op; 2,071,840 B/op; 12,319 allocs/op |
| Improvement | `1.27x` faster; `1.84x` lower bytes; `1.67x` fewer allocations |

The fallback control remains available for compatibility and uses the same
input rows. The native path does not introduce a process-wide cache or retain
rows between queries.

Reproduce with `make benchmark-m052z-unbounded-order`.
