# Typed SQL Tables

`hatSql.TypedTable` is an opt-in in-memory table for fixed schemas that need
compact primitive storage and exact changefeed-driven aggregates. Existing
cache-backed `Row` sources and SQL resolvers keep their current behavior.

Each configured column is stored in a type-specific slice (`string`, `int64`,
`float64`, or `bool`) with a separate validity bitmap. `Upsert` requires one
schema-ordered value per column and records an immutable before/after change.
The table can be passed directly to existing SQL execution as a `CACHE` source
whose key is the schema name.

## Setup

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
	Name: "events", // queried as CACHE('events')
	Columns: []hatSql.TypedTableColumn{
		{Name: "team", Kind: hatSql.TypedTableString},
		{Name: "points", Kind: hatSql.TypedTableInt64},
	},
})
if err != nil {
	return err
}

change, err := table.Upsert("event-42", []hatSql.TypedTableValue{
	hatSql.TypedString("blue"),
	hatSql.TypedInt64(5),
})
if err != nil {
	return err
}
_ = change
```

`TypedNull()` represents SQL `NULL`. `Delete` produces a change with `After`
unset. The table uses O(columns) swap-delete, so callers must use SQL `ORDER
BY` when row order matters.

## Exact Delta Aggregates

`TypedTableAggregate` consumes strictly ordered table changes. It maintains
grouped `count` and an optional numeric `sum` without rescanning all table
rows.

```go
aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{
	GroupBy:  []string{"team"},
	SumField: "points",
})
if err != nil {
	return err
}

changes, _, err := table.ChangesAfter(aggregate.Checkpoint(), 256)
if err != nil {
	return err
}
if err := aggregate.Apply(changes); err != nil {
	return err
}
rows := aggregate.Rows() // team, count, sum
_ = rows
```

Changes are idempotent at or below the aggregate checkpoint. A sequence gap is
rejected rather than skipped. Keep a durable aggregate checkpoint alongside
your consumer state before calling `CompactChangesThrough`; if `ChangesAfter`
returns `ErrTypedTableChangesCompacted`, rebuild the aggregate from a trusted
table snapshot.

## SQL Compatibility

The table implements both `SourceResolver` and `ColumnarSourceResolver`.
Existing queries work unchanged:

```sql
FROM CACHE('events') SELECT team WHERE points = 5
```

The columnar resolver avoids building source row maps for the established
single-source SQL fast path. Unsupported query shapes retain the existing row
resolver behavior.

## Tradeoff And Measurement

The default remains the existing cache/row source. Use `TypedTable` only for a
fixed, schema-controlled dataset where its explicit per-row API is acceptable.
The changefeed keeps before/after values until compacted, so its retention must
be sized and managed like a journal.

On an AMD Ryzen 9 5950X, `make benchmark-sql-typed-table` measured the median
of three runs as follows:

| Workload | Time/op | Heap/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Exact aggregate, one changed row | 743 ns | 958 B | 8 |
| Full rescan aggregate, 10,000 rows | 2.82 ms | 4.64 MB | 49,745 |

For this repeated-aggregate workload, delta maintenance is approximately
`3,794x` faster, uses `4,843x` less heap, and performs `6,218x` fewer
allocations. Those are workload-specific measurements, not a promise for
ad-hoc SQL queries.

## Verification

```sh
make test-sql-typed-table
make test-race-sql-typed-table
make benchmark-sql-typed-table
```
