# SQL Intermediate Row Limits

`SQLQueryOptions.MaxIntermediateRows` is an opt-in ClickHouse-inspired
complexity guard for queries that can expand rows through joins, array joins,
ASOF joins, unions, recursive CTEs, or other streamed stages.

The default is `0`, which leaves existing behavior unchanged. A positive value
tightens the executor's existing `MaxRows` bound. The smaller of the two
limits wins, and the guard rejects the query with a row-limit error rather than
truncating results. Because the same bound is used by source and intermediate
operators, a source can also be rejected before expansion; this is deliberate
and keeps the limit conservative.

```go
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	MaxIntermediateRows: 100_000,
})
```

For multi-tenant deployments, apply a namespace-wide ceiling before execution:

```go
policy := hatSql.NamespaceResourceLimits{
	MaxIntermediateRows: 50_000,
}
options = policy.Apply(options)
```

Queries with a non-zero intermediate-row limit bypass the SQL result cache so a
previously cached unrestricted result cannot bypass the guard. The setting is
an API and namespace-policy option; it is not enabled by default.
