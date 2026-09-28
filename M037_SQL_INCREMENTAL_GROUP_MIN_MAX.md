# M037 SQL Incremental Group MIN/MAX

`CompiledSQLQuery.CompileIncrementalGroupAggregate` now supports an opt-in
restricted grouped query with integer `MIN` and/or `MAX` projections:

```go
compiled, err := hatSql.CompileSQLQuery(
    "FROM CACHE('items') AS src " +
        "SELECT src.group, MIN(src.value) AS lowest, " +
        "MAX(src.value) AS highest GROUP BY src.group",
)
operator, err := compiled.CompileIncrementalGroupAggregate()
changes, err := operator.Apply([]hatSql.DifferentialRow{
    {Key: "item-1", Diff: 1, Row: hatSql.Row{"group": "red", "value": int64(5)}},
})
snapshot := operator.Snapshot()
```

## Supported Shape

- One `CACHE(...)` or `VALUES(...)` row source.
- One direct `GROUP BY` field that is projected.
- One direct integer field used by `MIN`, `MAX`, or both.
- Optional scalar `WHERE` without custom functions.
- No joins, CTEs, unions, ordering, limits, windows, `HAVING`, or mixed
  `COUNT`/`SUM` aggregates.

The operator retains value multiplicities per group. Removing a duplicate that
does not change either endpoint emits no row; removing the final endpoint emits
the exact retraction/insertion transition, and removing the last group emits a
retraction. Invalid integer values, negative multiplicities, and overflow are
rejected atomically without changing prior state.

The ordinary SQL executor remains the fallback for unsupported shapes. The
feature is opt-in, single-writer, and does not change default query execution.

## Benchmark

Command: `make benchmark-m037-sql-incremental-group-minmax`

One `-benchmem` sample on Linux/amd64, AMD Ryzen 9 5950X, using the existing
10,000-row grouped MIN/MAX workload:

| Path | ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Rebuild grouped MIN/MAX from 10,000 rows | 549,264 | 939,695 | 5,386 | baseline |
| Warm two-update incremental insert/retract | 2,413 | 2,052 | 20 | 227.6x faster, 457.9x lower bytes, 269.3x fewer allocs |

The tradeoff is retained per-group value multiplicity state. Memory therefore
grows with the number of distinct values in each active group; this is why the
path is explicit rather than silently replacing the normal executor.
