# M241 Optimizer Trace

`SQLQueryOptions.OptimizerTrace` enables a bounded, versioned trace on the
materialized `QueryResult` returned by `ExecuteSQLQuery` and its context
wrapper. It is disabled by default.

```go
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	Optimizer: hatSql.NewSQLQueryOptimizer(
		func(plan *hatSql.SQLQueryOptimizationContext) error {
			plan.IndexHint = hatSql.SQLIndexHint{
				Source: "orders",
				Field:  "customer_id",
				Mode:   hatSql.SQLIndexHintForce,
			}
			return nil
		},
	),
	OptimizerTrace: &hatSql.SQLOptimizerTraceOptions{MaxEntries: 64},
})
```

The returned `result.OptimizerTrace` contains:

- `format`: `hatrie-cache-sql-optimizer-trace/v1`;
- ordered rule entries with before/after hints and `applied`, `no_change`,
  `skipped`, or `error` status;
- rejected planner alternatives and machine-readable notices already produced
  by the explain metrics path;
- the final effective index hint when one exists; and
- `truncated: true` when the configured bound prevents retaining every trace
  entry or diagnostic.

`MaxEntries <= 0` uses 64 entries and values above 1,024 are capped. The trace
does not retain SQL text or arbitrary rule-owned state. Optimizer rule errors
still abort execution with the existing error contract; the entries recorded
before the error remain available in the returned result.

Tracing bypasses the SQL result cache so a cached result cannot hide a
requested trace. Stored and subscription result clones deep-copy the trace.
Streaming `ExecuteSQLQueryRows` has no materialized result on which to attach
the trace and therefore does not emit one.
