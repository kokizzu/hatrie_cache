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

The bounded path applies only to direct, unpartitioned, unordered windows:

- running `ROW_NUMBER`, `RANK`, and `DENSE_RANK`;
- running `SUM`, `AVG`, `MIN`, and `MAX` over one expression;
- `LAG` with a literal offset and optional literal/default expression;
- `LEAD` with a literal offset and optional default expression.

The query must not use joins, grouping, `HAVING`, `DISTINCT`, a query-level
`ORDER BY`, set operations, CTEs, window expressions in `WHERE`, or custom
functions in the streamed expression shape. `PARTITION BY`, window `ORDER BY`,
and explicit frames remain outside this path. Those cases retain the existing
materialized behavior where available; `ExecuteSQLQueryRows` does not silently
materialize an unsupported query.

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

## Verification

```text
make test-chu05-c245
make test-chu05-package-c245
make race-chu05-c245
make vet-chu05-c245
make benchmark-chu05-c245
make memory-chu05-c245
```

All focused tests, package tests, race tests, and vet pass. The benchmark and
memory scripts remove their temporary source mirrors and output directories on
exit. The repository cleanup preview remains available through
`make cleanup-test-tmp-preview`.
