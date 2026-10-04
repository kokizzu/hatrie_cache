# CH-G18 Approximate Percentile Metadata

`APPROX_PERCENTILE(value, quantile [, epsilon])` remains the compatible scalar
aggregate. The new opt-in `APPROX_PERCENTILE_INFO` variant returns a
`SQLApproxPercentileInfo` value with:

- `value`: the estimated percentile value;
- `quantile`: the requested quantile;
- `count`: finite numeric observations retained by the sketch;
- `epsilon`: the configured rank-error fraction; and
- `rank_error`: the conservative absolute rank bound, in observations.

Null and non-numeric values follow the existing percentile behavior. An empty
group returns `NULL`. Direct source expressions use the same bounded streaming
sketch state as `APPROX_PERCENTILE`; expressions that need the generic grouping
fallback use the same shared sketch builder.

The default scalar function and its result shape are unchanged. The metadata
variant is useful when a dashboard or operator needs to display approximation
quality instead of treating an estimated value as exact.

## Measurement

All samples ran on an AMD Ryzen 9 5950X, Linux amd64, with `-benchmem` and five
samples per workload. The clean baseline worktree was based on
`origin/codex/next-inspiration-m091-20261004`.

| Workload | Baseline | Final | Result |
| --- | ---: | ---: | --- |
| 10,000-row mixed approximate query, time | 8,271,192 ns/op | 8,087,230 ns/op | 1.02x faster |
| 10,000-row mixed approximate query, heap | 5,126,638 B/op | 5,126,614 B/op | effectively unchanged |
| 10,000-row mixed approximate query, allocations | 60,060 | 60,063 | +3, within noise |
| Percentile-only scalar, time | 2,691,566 ns/op | control | paired control |
| Percentile-only info, time | control | 2,657,452 ns/op | CPU-neutral within run noise |
| Percentile-only info, heap | control | 18,588 B/op | +39 B/op versus scalar |
| Percentile-only info, allocations | control | 38 allocs/op | unchanged versus scalar |

The extra metadata is therefore opt-in: the normal scalar path has no measured
memory or allocation cost from the result fields, while callers that request
metadata pay only the small result-shape cost.
