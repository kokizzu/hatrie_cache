# M210 Retained SQL Snapshots

M210 connects the existing `TypedTable` MVCC history to the SQL `AS OF`
frontier provider. It is an opt-in retained-state feature inspired by
Materialize historical reads and ClickHouse snapshot-oriented execution.

## API

`TypedTable` now implements `SQLFrontierSnapshotProvider` when its schema has
`MVCC.Enabled: true`:

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
	Name: "users",
	MVCC: hatSql.TypedTableMVCCOptions{Enabled: true},
	Columns: []hatSql.TypedTableColumn{
		{Name: "name", Kind: hatSql.TypedTableString},
	},
})
if err != nil {
	panic(err)
}

frontier := uint64(7)
result, err := hatSql.ExecuteSQLQueryContext(
	context.Background(),
	"FROM CACHE('users') SELECT name",
	table,
	hatSql.SQLQueryOptions{AsOfFrontier: &frontier},
)
```

For a query that reads multiple tables, register all tables in a
`TypedTableSQLSnapshotRegistry`. The registry captures every registered table
at the same logical frontier and returns one immutable resolver view:

```go
registry := hatSql.NewTypedTableSQLSnapshotRegistry()
if err := registry.Register(users); err != nil {
	panic(err)
}
if err := registry.Register(orders); err != nil {
	panic(err)
}

result, err := hatSql.ExecuteSQLQueryContext(
	context.Background(),
	"FROM CACHE('users') SELECT name",
	registry,
	hatSql.SQLQueryOptions{AsOfFrontier: &frontier},
)
```

The registry requires MVCC-enabled tables and rejects duplicate
`SourceName`/`Name` identities. Tables registered together must share one
logical sequence domain; the registry does not invent a cross-table commit
protocol.

## Semantics

- Ordinary reads remain live and do not create a snapshot.
- `AS OF` captures immutable `TypedTableSnapshot` state before SQL execution.
- Later upserts do not change an already-open historical view.
- Row, columnar, borrowed-columnar, preference, and source-version resolver
  capabilities are preserved on the historical view.
- A frontier newer than a table's retained sequence, or older than explicit
  MVCC compaction, returns an error instead of silently serving partial state.
- A canceled context is checked before and during multi-table capture.
- MVCC and the registry are disabled unless explicitly configured; the
  compatibility default remains the live table path.

## Benchmark

Linux/amd64 on AMD Ryzen 9 5950X, five `-count=5` samples. Baselines use the
same retained table fixture and manually call `SnapshotAt`.

| Workload | Samples (ns/op) | Median ns/op | B/op | Allocs/op | Relative result |
| --- | --- | ---: | ---: | ---: | --- |
| One table, direct `SnapshotAt` baseline | 1002; 946.5; 949.2; 949.9; 960.5 | 949.9 | 1112 | 10 | 1.00x |
| One table, `BeginSQLSnapshotAt` | 1001; 858.1; 940.3; 1049; 977.7 | 977.7 | 1112 | 10 | 1.03x CPU cost; same bytes/allocs |
| Two tables, manual `SnapshotAt` baseline | 1825; 1943; 1945; 1791; 2110 | 1943 | 2224 | 20 | 1.00x |
| Two tables, `TypedTableSQLSnapshotRegistry` | 2173; 2272; 2205; 2220; 1948 | 2205 | 2336 | 22 | 1.13x CPU cost; 1.05x bytes; 1.10x allocs |

The feature is a correctness and capability addition, not a claim of faster
historical capture. Single-table use has no additional per-operation heap
cost. Multi-table use pays a small fixed resolver-view cost for one shared
frontier; ordinary live reads pay no registry cost unless callers opt in.

Run the measurements with:

```text
make benchmark-m210-retained-sql-snapshot-baseline
make benchmark-m210-retained-sql-snapshot
```
