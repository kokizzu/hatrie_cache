# SQL WITH FILL

`WITH FILL` generates missing time buckets in an ordered SQL result. The
current grammar intentionally accepts one ascending, selected time field and
literal bounds:

```sql
SELECT ts, SUM(value) AS total
FROM CACHE('events')
GROUP BY ts
ORDER BY ts WITH FILL
FROM TIMESTAMP '2026-01-01T00:00:00Z'
TO TIMESTAMP '2026-01-01T01:00:00Z'
STEP DURATION '1m'
```

The same gap-filling primitive is available to embedded callers:

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

The interval is half-open: `FROM` is included and `TO` is excluded. Existing
rows are cloned in order. Generated rows contain every selected column with a
`NULL` value, except the fill column, which contains the generated timestamp.
An `ORDER BY` alias can be used when that alias is the selected fill field.

For direct `FillSQLRows` calls, generated rows are copies of `Template` with
the time column replaced by the generated timestamp. The helper does not
mutate input rows or the template and returns errors for invalid bounds,
non-positive steps, missing or non-time columns, unordered rows, or values
outside the requested interval.

`MaxRows` applies to the expanded result, and `LIMIT`/`OFFSET` are applied
after filling. This prevents a query from allocating an unbounded range.
Invalid bounds, non-positive steps, descending or multi-key fill orders, and
an unselected fill field are rejected. `LIMIT BY` is currently rejected with
`WITH FILL` because its per-group semantics need a separate fill plan.

Fill queries use the materialized ordering path so indexed, columnar, grouped,
and external streaming shortcuts cannot omit generated rows. This preserves
correctness but can use more memory than a normal streaming query; the normal
`MaxRows`, `MaxResultBytes`, and sort budgets still apply.

## Benchmark

Run:

```text
make benchmark-with-fill-query
```

Measured on the repository benchmark host with 50 one-minute buckets and 25
input rows:

```text
BenchmarkSQLWithFill-32     23838  50582 ns/op  64180 B/op  460 allocs/op
BenchmarkSQLWithoutFill-32  34827  34441 ns/op  43081 B/op  314 allocs/op
```

The unfilled comparison returns 25 grouped rows while the fill query returns
50 rows, so its absolute totals are not an equal-output comparison. The fill
path's additional work is the expected 25 generated rows plus their cloned
maps; it is intentionally bounded rather than silently streaming an unlimited
range.
