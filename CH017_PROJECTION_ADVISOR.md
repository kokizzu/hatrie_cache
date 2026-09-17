# CH-17 Workload-Driven Projection Advisor

The optional `hatSql.SQLProjectionAdvisor` turns repeated slow `CACHE` queries into bounded, privacy-safe projection candidates. It records the caller's query ID, source dependencies, referenced fields, and the structural fields used by filters, grouping, and ordering. It does not retain SQL text, literals, rows, or automatically create storage.

```go
advisor := hatSql.NewSQLProjectionAdvisor(128)
options := hatSql.QueryOptions{
	ProjectionAdvisor:  advisor,
	QueryID:            "dashboard.team_totals",
	SlowQueryThreshold: 25 * time.Millisecond,
}

result, err := hatSql.ExecuteQueryParameters(ctx, query, resolver, nil, options)
_ = result
_ = err

for _, candidate := range advisor.CostRecommendations(10) {
	fmt.Printf("%s fields=%v filters=%v groups=%v order=%v\n",
		candidate.QueryID,
		candidate.Fields,
		candidate.FilterFields,
		candidate.GroupByFields,
		candidate.OrderByFields,
	)
}
```

`Fields` is the sorted union of fields referenced by the query, including join expressions. Qualified names retain their query alias, for example `events.team`. The category fields are sorted and deduplicated independently. Different structural shapes under the same query ID and source dependencies remain separate candidates.

Use the result as an input to an application-owned materialized-view or projection review process. Recommendations are advisory only, and a nonpositive advisor capacity disables retention. The query execution path has no advisor overhead when `ProjectionAdvisor` is nil.

## Safety Boundaries

- Recommendations are bounded by the capacity supplied to `NewSQLProjectionAdvisor`.
- A caller-supplied `QueryID` is required; generated IDs are never retained as projection candidates.
- Only queries whose sources are `CACHE` sources are considered.
- Queries that already use an index are excluded from projection advice.
- The advisor never executes DDL or changes query plans.

## Verification

Run `make test-ch017-projection-advisor` for focused behavior, `make test-ch017-package` for the complete SQL package, and `make race-ch017-projection-advisor` for the enabled advisor path.
