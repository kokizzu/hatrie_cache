# CH-012 Projection Advisor Cost Model

`SQLProjectionAdvisor.CostBasedRecommendations` is an opt-in planning aid for
deciding whether a projection is worth maintaining. It combines the advisor's
bounded observed query latency with caller-owned estimates for projection-hit
latency, initial build work, and refresh work.

It does not create projections, change query planning, retain SQL text or row
data, or persist workload history. The existing `Recommendations` and
`CostRecommendations` methods keep their behavior.

## Model

```go
recommendations, err := advisor.CostBasedRecommendations(10, hatSql.SQLProjectionCostModel{
	ExpectedQueries:   10_000,
	ExpectedRefreshes: 24,
	QueryHitLatency:   2 * time.Millisecond,
	InitialBuildCost:  400 * time.Millisecond,
	RefreshCost:       15 * time.Millisecond,
})
```

For each observed query shape:

```text
savings_per_query = max(observed_average_latency - query_hit_latency, 0)
query_savings     = savings_per_query * expected_queries
maintenance_cost  = initial_build_cost + refresh_cost * expected_refreshes
net_benefit       = query_savings - maintenance_cost
```

`WorthBuilding` is true only when `net_benefit` is positive. A positive
`limit` keeps only the highest-net-benefit candidates; a nonpositive limit
returns every candidate. `ExpectedQueries` must be positive, and all durations
must be nonnegative. Duration arithmetic saturates instead of overflowing.

The advisor uses the observed average of recorded slow queries. It cannot
infer future query volume, source write rate, or refresh cost, so those values
remain explicit operator inputs rather than hidden guesses.

## Operational behavior

- The advisor remains disabled unless a caller supplies `ProjectionAdvisor`.
- `CostBasedRecommendations` is read-only and safe to call concurrently with
  query feedback collection.
- Recommendations remain bounded by the advisor capacity.
- The returned dependency and workload-shape slices are independent copies.
- A cost recommendation is evidence for a caller-owned DDL decision; it is
  not an automatic `CREATE PROJECTION` command.

## Measurement

`make benchmark-ch012-projection-advisor-cost` compares the existing observed
latency ranking with the new cost-based ranking over 128 candidates and a
32-result limit. Five 1,000-iteration samples were run on Linux/amd64 with an
AMD Ryzen 9 5950X.

| Path | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing observed-cost ranking | 41,318 | 24,040 | 132 | Baseline |
| Cost-based ranking | 41,816 | 29,576 | 132 | 1.01x time, 1.23x heap, same allocations |

The remaining heap increase is the additional cost fields returned for each
candidate. The first implementation also allocated an intermediate
recommendation slice; that version was rejected by the benchmark and replaced
with a direct construction path before the feature was retained. Since the
feature is explicit and does not run in the default query path, the measured
cost is paid only by callers requesting the richer analysis.

Focused correctness coverage is in
`hat/hatSql/ch012_projection_advisor_cost_test.go`, including invalid models,
limit handling, and saturating duration arithmetic.
