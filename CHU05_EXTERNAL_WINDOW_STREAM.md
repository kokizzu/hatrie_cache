# CH-U05 External Window Streaming

CH-U05 extends `ExecuteSQLQueryRows` to stream a narrow, bounded-state window
subset from a direct `EXTERNAL('name')` source. The external resolver must
implement both `SQLSourceResolver` and the optional
`ExternalStreamSourceResolver` interface:

```go
type ExternalStreamSourceResolver interface {
	StreamSQLExternalSource(context.Context, string, func(Row) error) error
}
```

The resolver callback is consumed in source order. The executor does not build
the materialized result-row slice. Existing materialized execution and
resolvers that only implement `ResolveSQLExternalSource` are unchanged.

## Supported Subset

The bounded path applies only to direct, unordered windows:

- running `ROW_NUMBER`, `RANK`, and `DENSE_RANK`;
- running `SUM`, `AVG`, `MIN`, and `MAX` over one expression;
- `LAG` with a literal offset and optional literal/default expression;
- `LEAD` with a literal offset and optional default expression.

`PARTITION BY` is supported for the running-window subset, including composite
partition expressions. Each distinct observed partition retains its own scalar
state or fixed `LAG` history. The source order is preserved and state is
bounded by the configured input-row limit, but high partition cardinality still
retains one small state map entry per partition.

The query must not use joins, grouping, `HAVING`, `DISTINCT`, a query-level
`ORDER BY`, set operations, CTEs, window expressions in `WHERE`, or custom
functions in the streamed expression shape. Window `ORDER BY` and explicit
frames remain outside this path. Those cases retain the existing materialized
behavior where available; `ExecuteSQLQueryRows` does not silently materialize
an unsupported query.

Running windows keep scalar state plus a fixed history for `LAG`. `LEAD` keeps
only the pending rows needed by its largest literal offset. The source callback
must honor context cancellation; the executor also checks its normal row,
result-byte, and execution controls.

## Benchmark

The benchmark uses the same generated 4,096-row external source and query for
both APIs:

```sql
FROM EXTERNAL('events') AS event
SELECT event.id,
       ROW_NUMBER() OVER () AS row_number,
       SUM(event.value) OVER () AS running_sum,
       LAG(event.value) OVER () AS previous_value
```

Five `-count=5` runs were measured on an AMD Ryzen 9 5950X. The materialized
column is the existing `ExecuteSQLQueryContext` baseline; streaming is the new
`ExecuteSQLQueryRows` path.

| Metric | Materialized | Streaming | Materialized / streaming |
| --- | ---: | ---: | ---: |
| Median time | 362.14 ms/op | 6.56 ms/op | 55.2x faster |
| Median cumulative allocation | 251,763,026 B/op | 4,337,313 B/op | 58.0x lower |
| Median allocation count | 87,217 allocs/op | 69,414 allocs/op | 1.26x fewer |
| One-shot maximum RSS | 34,164 KiB | 24,620 KiB | 1.39x lower, 27.9% lower |

`B/op` is cumulative allocation volume, not retained heap. RSS includes the Go
runtime and test-process overhead, so it is directional rather than a direct
per-query resident-size limit. The result-equality regression test compares
streamed rows with materialized rows, including fixed-offset `LAG` behavior.

Raw benchmark output:

```text
BenchmarkCHU05ExternalWindowMaterialized-32       3  350640709 ns/op  251764890 B/op  87219 allocs/op
BenchmarkCHU05ExternalWindowMaterialized-32       3  353817799 ns/op  251762882 B/op  87216 allocs/op
BenchmarkCHU05ExternalWindowMaterialized-32       3  362137328 ns/op  251763042 B/op  87217 allocs/op
BenchmarkCHU05ExternalWindowMaterialized-32       3  374866016 ns/op  251762968 B/op  87216 allocs/op
BenchmarkCHU05ExternalWindowMaterialized-32       3  394284699 ns/op  251763026 B/op  87217 allocs/op
BenchmarkCHU05ExternalWindowStreaming-32        181    6730205 ns/op    4337378 B/op  69414 allocs/op
BenchmarkCHU05ExternalWindowStreaming-32        180    6576552 ns/op    4337290 B/op  69413 allocs/op
BenchmarkCHU05ExternalWindowStreaming-32        189    6557837 ns/op    4337398 B/op  69415 allocs/op
BenchmarkCHU05ExternalWindowStreaming-32        188    6263779 ns/op    4337306 B/op  69414 allocs/op
BenchmarkCHU05ExternalWindowStreaming-32        195    6306755 ns/op    4337313 B/op  69414 allocs/op
```

## Partitioned Extension Benchmark

The partitioned extension uses 4,096 rows and 64 interleaved partitions:

```sql
FROM EXTERNAL('events') AS event
SELECT event.id, event.group,
       ROW_NUMBER() OVER (PARTITION BY event.group) AS row_number,
       SUM(event.value) OVER (PARTITION BY event.group) AS running_sum,
       LAG(event.value) OVER (PARTITION BY event.group) AS previous_value
```

The pre-change materialized baseline was measured before the partitioned
executor existed. The post-change materialized column is a same-run control;
the streaming column is the new executor. Five `-count=5` samples used
`GOMAXPROCS=1` on the same AMD Ryzen 9 5950X host.

| Metric | Before materialized | After materialized control | After partitioned streaming | Streaming vs control |
| --- | ---: | ---: | ---: | ---: |
| Median time | 14.94 ms/op | 14.08 ms/op | 10.06 ms/op | 1.40x faster |
| Cumulative allocation | 7,235,239 B/op | 7,235,235 B/op | 5,771,232 B/op | 1.25x lower, 20.2% lower |
| Allocation count | 67,384 allocs/op | 67,384 allocs/op | 86,342 allocs/op | 1.28x higher |

The win is lower latency and cumulative allocation bytes, not fewer allocation
events. The additional small objects are partition-key/state bookkeeping; the
executor avoids retaining the full source/result row set. Explicitly ordered
or framed windows remain on the materialized path.

Raw pre-change output:

```text
BenchmarkCHU05ExternalPartitionedWindowMaterialized       15  14941096 ns/op  7235249 B/op  67385 allocs/op
BenchmarkCHU05ExternalPartitionedWindowMaterialized       18  14684014 ns/op  7235233 B/op  67384 allocs/op
BenchmarkCHU05ExternalPartitionedWindowMaterialized       16  14811587 ns/op  7235224 B/op  67384 allocs/op
BenchmarkCHU05ExternalPartitionedWindowMaterialized       15  15673831 ns/op  7235239 B/op  67385 allocs/op
BenchmarkCHU05ExternalPartitionedWindowMaterialized       15  16801994 ns/op  7235212 B/op  67384 allocs/op
```

Raw post-change output:

```text
BenchmarkCHU05ExternalPartitionedWindowMaterialized       16  14600403 ns/op  7235242 B/op  67384 allocs/op
BenchmarkCHU05ExternalPartitionedWindowMaterialized       16  15460949 ns/op  7235234 B/op  67385 allocs/op
BenchmarkCHU05ExternalPartitionedWindowMaterialized       16  13817661 ns/op  7235237 B/op  67384 allocs/op
BenchmarkCHU05ExternalPartitionedWindowMaterialized       16  14084260 ns/op  7235235 B/op  67384 allocs/op
BenchmarkCHU05ExternalPartitionedWindowMaterialized       18  13978211 ns/op  7235230 B/op  67384 allocs/op
BenchmarkCHU05ExternalPartitionedWindowStreaming          25  11090065 ns/op  5771232 B/op  86342 allocs/op
BenchmarkCHU05ExternalPartitionedWindowStreaming          20  11592095 ns/op  5771237 B/op  86342 allocs/op
BenchmarkCHU05ExternalPartitionedWindowStreaming          22   9899060 ns/op  5771227 B/op  86342 allocs/op
BenchmarkCHU05ExternalPartitionedWindowStreaming          24  10059471 ns/op  5771236 B/op  86342 allocs/op
BenchmarkCHU05ExternalPartitionedWindowStreaming          25   9731139 ns/op  5771231 B/op  86342 allocs/op
```

## Verification

```text
make test-chu05-partitioned-window
make test-chu05-partitioned-window-package
make race-chu05-partitioned-window
make vet-chu05-partitioned-window
make benchmark-chu05-partitioned-window
```

All focused tests, package tests, race tests, and vet pass. These targets use
`scripts/chu05-partitioned-window.sh`; they do not create build artifacts in
`/tmp`.
