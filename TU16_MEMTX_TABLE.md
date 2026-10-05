# T-U16 Memtx-Style Row Engine

`hatSql.MemtxTable` is an opt-in, Tarantool-inspired in-memory tuple engine
for workloads dominated by primary-key mutations and equality lookups. It is
selected explicitly with `NewMemtxTable`; the existing `TypedTable` remains
the default column-oriented engine and is unchanged.

```go
table, err := hatSql.NewMemtxTable(hatSql.MemtxTableSchema{
	Name: "events",
	Columns: []hatSql.TypedTableColumn{
		{Name: "region", Kind: hatSql.TypedTableString},
		{Name: "score", Kind: hatSql.TypedTableInt64},
	},
	Indexes: []hatSql.MemtxIndexDefinition{
		{Name: "events_region", Field: "region"},
	},
})
if err != nil {
	return err
}
_, err = table.Upsert("event-1", []hatSql.TypedTableValue{
	hatSql.TypedString("apac"),
	hatSql.TypedInt64(42),
})
```

The string primary key is always indexed. Optional scalar equality indexes
implement `hatSql.IndexedSourceResolver`, so ordinary SQL equality predicates
can use them after the executor's normal predicate recheck:

```sql
FROM CACHE('events')
SELECT region, score
WHERE region = 'apac'
```

Rows are stored as dense tuples. Deletes swap the final tuple into the removed
position and update all affected indexes, keeping row storage dense. `Get`,
`Rows`, `ResolveSQLSource`, exact cardinality, indexed candidates, copy-safe
mutation records, and a logical `MemoryUsage` report are provided.

The engine deliberately rejects generated columns, TTL columns, dictionary
encoding, and columnar-cache options instead of silently changing their
semantics. Use `TypedTable` when those features or columnar scans, aggregates,
MVCC, patch parts, or TTL processing are required. `LogicalBytes` is an
admission estimate; it is not a substitute for process RSS or heap profiling.

## Measurement

Five `100ms` `-benchmem` samples were run on Linux/amd64, AMD Ryzen 9 5950X.
The fixture uses two scalar columns and 1,024 rows where applicable.

| Workload | TypedTable control median | MemtxTable median | Result |
| --- | ---: | ---: | --- |
| Repeated upsert | 690.8 ns; 872 B; 4 allocs | 376.5 ns; 296 B; 4 allocs | 1.83x faster; 2.95x lower bytes; equal allocations |
| Materialize 1,024 rows | 328,519 ns; 474,372 B; 4,865 allocs | 217,658 ns; 376,067 B; 3,841 allocs | 1.51x faster; 1.26x lower bytes; 1.27x fewer allocations |

For one equality point lookup, the indexed row engine measured `440.0 ns/op`,
`381 B/op`, and 6 allocations. The old full-scan control measured `383,904
ns/op`, `474,372 B/op`, and 4,865 allocations: about 875x faster, 1,245x
lower bytes, and 811x fewer allocations in this selective fixture.

Raw samples:

```text
memtx_upsert: 391.8 358.8 372.3 376.5 378.5 ns/op; 296 B/op; 4 allocs/op
typed_upsert: 842.5 676.6 690.8 708.7 655.9 ns/op; 911 872 838 900 856 B/op; 4 allocs/op
memtx_rows: 226055 223651 216340 217658 213897 ns/op; 376067-376068 B/op; 3841 allocs/op
typed_rows: 314262 328519 327811 340064 345704 ns/op; 474373-474374 B/op; 4865 allocs/op
memtx_indexed_lookup: 424.8 448.8 462.4 413.9 440.0 ns/op; 381 B/op; 6 allocs/op
typed_full_scan_lookup: 346364 399689 396909 358445 385174 ns/op; 474372-474374 B/op; 4865 allocs/op
```

The feature is retained because it improves both mutation and row materializing
workloads without increasing allocations for the hot upsert path, while the
existing columnar engine remains the safe default for analytical workloads.
