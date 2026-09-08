# SQL Optimizer Rules

`hatSql` exposes an opt-in optimizer rule hook for applications that already
know a safe planner decision for a query shape. Rules run after parsing and
before source resolution. They receive the raw query source and a structural
copy of the existing `EXPLAIN` steps, and may select an existing
`SQLIndexHint`.

```go
optimizer := hatSql.NewSQLQueryOptimizer(func(plan *hatSql.SQLQueryOptimizationContext) error {
	for _, step := range plan.Plan {
		if step.Node == "SCAN" && strings.Contains(step.Detail, `CACHE("orders")`) {
			plan.IndexHint = hatSql.SQLIndexHint{
				Source: "o",
				Field:  "id",
				Mode:   hatSql.SQLIndexHintForce,
			}
		}
	}
	return nil
})

result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	Optimizer: optimizer,
})
```

Rules are ordered and can be shared between concurrent queries. A rule error
stops the query before source access. Invalid hint output is rejected. An
explicit `SQLQueryOptions.IndexHint` wins over a rule-produced hint. A nil
optimizer, an empty optimizer, or the default query API does not run rules and
keeps the existing planner path.

This is deliberately a narrow extension point. It does not expose the private
SQL AST, rewrite SQL text, add indexes, or introduce a new execution operator.
Rules should be used only for an application-controlled decision that can be
expressed by the existing index-hint contract. `EXPLAIN ANALYZE` remains the
source of truth for verifying the selected plan.

## Cost

The rule path is opt-in and has a measurable per-query cost because it builds a
structural plan copy. On the local AMD Ryzen 9 5950X benchmark using
`FROM VALUES (1) AS values(id) SELECT id`, three samples gave:

| Mode | Median time | Heap | Allocs |
| --- | ---: | ---: | ---: |
| Default | 2,863 ns/op | 3,312 B/op | 25/op |
| One no-op rule | 3,921 ns/op | 5,304 B/op | 32/op |

The no-op rule is approximately `1.37x` slower, uses `1.60x` the heap, and
uses 7 more allocations. This cost is paid only when an optimizer is supplied;
the default path does not build the rule context.
