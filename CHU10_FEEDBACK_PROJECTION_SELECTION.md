# Feedback-Driven Projection Selection

CH-U10 extends the existing opt-in SQL projection advisor with bounded
execution-cost feedback. The advisor still keeps only a caller-supplied query
ID and sorted `CACHE` dependency names; it does not retain SQL text, literal
values, parameters, or result rows.

```go
advisor := hatSql.NewSQLProjectionAdvisor(128)
options := hatSql.QueryOptions{
	QueryID:            "team_totals_dashboard",
	SlowQueryThreshold: 50 * time.Millisecond,
	ProjectionAdvisor:  advisor,
}

_, err := hatSql.ExecuteQueryParameters(ctx, query, resolver, nil, options)
if err != nil {
	return err
}

// Choose which application-managed projection to create or refresh first.
for _, candidate := range advisor.CostRecommendations(10) {
	fmt.Printf("%s: %s total, %s average\n", candidate.QueryID, candidate.TotalElapsed, candidate.AverageElapsed)
}
```

Successful queries that meet `SlowQueryThreshold` contribute one observation.
Queries without `QueryID`, failed queries, non-`CACHE` sources, and queries
already using an index are not retained. `TotalElapsed` estimates the workload
cost of a candidate (`frequency * observed latency`); `AverageElapsed` shows
the typical slow execution. Counters saturate instead of wrapping.

`Recommendations()` remains count-ranked for compatibility. The new
`CostRecommendations(limit)` method ranks by total elapsed time, then average
elapsed time, count, query ID, and dependency list. A nonpositive limit returns
all candidates. The method is deterministic, concurrent-safe, and does not
alter SQL planning, create a view, start a worker, or select a projection
automatically.

The feature is disabled by default: leave `ProjectionAdvisor` nil to retain no
advisor state. When enabled, the existing execution path only performs the
bounded advisor update. Cost sorting and its allocations happen only when
`CostRecommendations` is called.

## Measurement

Measured on an AMD Ryzen 9 5950X, Linux/amd64, with
`-benchmem -benchtime=200ms -count=5` from a clean archive:

| Workload | Before | After | Result |
|---|---:|---:|---|
| Advisor disabled | 4.34-4.55 us/op; 5,192 B/op; 22 allocs | 4.38-4.57 us/op; 5,192 B/op; 22 allocs | No measurable regression |
| Advisor enabled, one slow candidate | 6.99-7.56 us/op; 6,584 B/op; 37 allocs | 7.11-7.24 us/op; 6,584 B/op; 37 allocs | Within run variance; no new allocation |
| Rank 128 candidates and return 16 | Not available | 17.52-18.96 us/op; 11,656 B/op; 132 allocs | Explicit operator/reporting cost only |

Run the focused checks with:

```sh
make test-chu10-c250
make benchmark-chu10-c250
```
