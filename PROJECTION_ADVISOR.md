# SQL Projection Advisor

`hatSql.SQLProjectionAdvisor` is an opt-in, bounded helper that identifies
repeated slow CACHE queries suitable for an application-managed materialized
projection. It never creates a view, starts a worker, or retains SQL text,
literals, parameters, or result rows.

The caller must provide a stable, non-sensitive `QueryID`. Recommendations
contain only that ID, the sorted CACHE dependency names, and a slow-query
count. Use the ID to find the application-owned query definition, then create
and operate a materialized view explicitly.

```go
advisor := hatSql.NewSQLProjectionAdvisor(32)
options := hatSql.QueryOptions{
	QueryID:            "team_totals_dashboard",
	SlowQueryThreshold: 50 * time.Millisecond,
	ProjectionAdvisor:  advisor,
}

result, err := hatSql.ExecuteQueryParameters(ctx, query, resolver, nil, options)
if err != nil {
	return err
}
_ = result

for _, recommendation := range advisor.Recommendations() {
	// Map QueryID to an application-owned query before defining a view.
	_ = recommendation
}
```

The zero value is disabled: no advisor state or query-path work is added when
`ProjectionAdvisor` is nil. A nonpositive advisor capacity is inert. Failed,
fast, unlabeled, or non-CACHE queries are not retained.

## Cost

On an AMD Ryzen 9 5950X, `make benchmark-sql-projection-advisor` measured the
median of three simple query runs as 4.00 us and 4,976 B/op with the advisor
disabled, versus 4.27 us and 5,000 B/op enabled. That is about 7% extra CPU,
24 B/op, and two allocations for the explicit operational signal.

## Verification

```sh
make test-sql-projection-advisor
make test-race-sql-projection-advisor
make benchmark-sql-projection-advisor
```
