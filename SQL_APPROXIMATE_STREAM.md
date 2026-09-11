# Streaming Approximate Aggregate State

This is the CH-039 ClickHouse-inspired execution improvement for approximate
distinct and quantile aggregates. It keeps the existing HyperLogLog and
quantile-sketch algorithms and changes only how eligible global queries feed
them.

## Supported Path

The result and row-streaming SQL APIs use bounded aggregate state for global,
ungrouped, non-window queries containing only:

- `APPROX_COUNT_DISTINCT(value [, precision])`
- `APPROX_PERCENTILE(value, quantile [, epsilon])`

Rows are consumed through `SQLStreamSourceResolver` when available, so the
executor does not retain a materialized `[]sqlExecRow`. Direct field and
literal arguments use the source row directly and avoid a per-row execution
map. `WHERE` and aggregate `FILTER` expressions remain supported through the
general stream evaluator.

Queries with grouping, `HAVING`, joins, windows, sampling, `LIMIT BY`,
`WITH TIES`, typed source fields, `APPROX_TOP_K`, invalid sketch arguments, or
other unsupported shapes retain the established evaluator or rejection path.
This keeps exact validation and the existing approximate result types intact.

## Benchmark

The workload uses 10,000 rows, 4,000 repeating visitor IDs, one HLL aggregate
with precision `10`, and one percentile aggregate with epsilon `0.01`. The
benchmark uses the same `ExecuteSQLQuery` API and query before and after the
change. The pre-change resolver exposed only materialized source resolution;
the post-change resolver exposes the existing streaming callback contract.
Each result is one row. Values are checked by the feature tests against the
existing materialized evaluator.

Ten `-benchmem` samples were collected on an AMD Ryzen 9 5950X. Medians:

| Path | ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Before, materialized source | 6,562,801 | 4,684,427 | 40,230 | baseline |
| After, direct streaming state | 4,347,983 | 193,019 | 20,157 | 1.509x faster, 24.269x less heap, 1.996x fewer allocations |

Raw pre-change output from `make benchmark-ch039-approx-stream`:

```text
6673456 ns/op 4684286 B/op 40228 allocs/op
6568300 ns/op 4684450 B/op 40230 allocs/op
6669351 ns/op 4684583 B/op 40230 allocs/op
6518109 ns/op 4684370 B/op 40228 allocs/op
6468033 ns/op 4684403 B/op 40229 allocs/op
6561648 ns/op 4684237 B/op 40227 allocs/op
6509151 ns/op 4684746 B/op 40233 allocs/op
6474060 ns/op 4683639 B/op 40221 allocs/op
6563953 ns/op 4683831 B/op 40223 allocs/op
6570719 ns/op 4684451 B/op 40230 allocs/op
```

Raw post-change output from `make benchmark-ch039-approx-stream`:

```text
4342659 ns/op 193318 B/op 20160 allocs/op
4343402 ns/op 193065 B/op 20157 allocs/op
4335609 ns/op 193116 B/op 20157 allocs/op
4328339 ns/op 192872 B/op 20155 allocs/op
4352564 ns/op 193067 B/op 20157 allocs/op
4318578 ns/op 192824 B/op 20154 allocs/op
4373709 ns/op 193021 B/op 20157 allocs/op
4356227 ns/op 193017 B/op 20156 allocs/op
4362350 ns/op 192826 B/op 20154 allocs/op
4354967 ns/op 192827 B/op 20154 allocs/op
```
