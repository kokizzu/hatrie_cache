# Ordered Time-Series Gap Filling

`hatSql.FillSQLRows` provides the execution primitive for ClickHouse-style
`WITH FILL` behavior over materialized SQL rows. It fills missing time steps
without changing the existing query executor or parser:

```go
filled, err := hatSql.FillSQLRows(rows, hatSql.SQLWithFillSpec{
	Column: "at",
	From:   start,
	To:     end,
	Step:   time.Hour,
	Template: hatSql.Row{
		"series": "cpu",
		"value":  int64(0),
	},
})
```

`From` is inclusive and `To` is exclusive. Existing rows must be ordered by
the named column, contain `time.Time` values, and lie inside the interval.
Rows on the regular grid are retained; rows between grid points are also
retained and do not cause a later grid point to be skipped. Generated rows are
copies of `Template` with the time column replaced by the generated timestamp.

The function does not mutate input rows or the template. It returns an error
for invalid bounds, a non-positive step, missing or non-time columns, unordered
rows, or values outside the requested interval. The half-open interval makes
adjacent fill ranges compose without duplicating their boundary.

This is additive and wire/storage neutral. Existing SQL queries continue to
behave exactly as before; a query planner or client can opt into the helper
after an ordered result is available.
