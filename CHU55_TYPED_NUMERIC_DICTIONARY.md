# CH-U55 Typed Numeric Low-Cardinality Dictionary

`TypedTableColumn.DictionaryEncoded` now supports `TypedTableInt64` in addition
to its existing string path. Repeated integer values are stored as `uint32`
row codes plus a compact dictionary of distinct `int64` values. This is useful
for status codes, tenant IDs, region IDs, and date/time values represented by
the typed table's integer column.

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
	Name: "events",
	Columns: []hatSql.TypedTableColumn{
		{Name: "region_id", Kind: hatSql.TypedTableInt64, DictionaryEncoded: true},
	},
})
```

The option is explicit and remains disabled by default. `TypedTableFloat64` and
`TypedTableBool` continue to use their existing primitive storage. String
dictionaries are unchanged. SQL NULL validity remains separate from the code,
so a NULL row never requires a dictionary value.

## Tradeoff

The benchmark used Linux/amd64 on an AMD Ryzen 9 5950X. Each sample inserted
2,048 rows through `Upsert`; the repeated workload used eight distinct values
and the unique workload used 2,048 distinct values. Five samples were collected
with `make benchmark-chu55`.

`payload-bytes/op` is the deterministic tracked slice payload for the typed
column (`valid` plus either primitive values or codes and dictionary values). It
does not include Go map bucket overhead, so the `B/op` column is also reported
for the complete benchmark path.

| Workload | Plain median ns/op | Dictionary median ns/op | Relative CPU | Plain payload | Dictionary payload | Plain B/op | Dictionary B/op | Plain allocs/op | Dictionary allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Upsert, 8 values | 906,879 | 905,674 | 1.00x | 18,432 | 10,304 | 1,295,071 | 1,256,526 | 4,183 | 4,190 |
| Upsert, 2,048 values | 872,965 | 1,117,441 | 0.78x | 18,432 | 26,624 | 1,295,070 | 1,485,640 | 4,183 | 4,238 |
| Upsert plus `Rows`, 8 values | 1,465,932 | 1,467,549 | 1.00x | 18,432 | 10,304 | 2,099,941 | 2,061,396 | 10,328 | 10,335 |
| Upsert plus `Rows`, 2,048 values | 1,493,946 | 1,707,346 | 0.88x | 18,432 | 26,624 | 2,114,279 | 2,304,852 | 12,120 | 12,175 |

The repeated workload cuts the tracked numeric payload by 44.1% with no
material CPU regression in this fixture. High-cardinality input is worse: the
dictionary payload is 44.4% larger and the upsert path is 1.29x slower before
map metadata is counted. Callers should therefore use this option only for
known low-cardinality integer columns; it is intentionally not automatic.

## Correctness Coverage

Focused tests cover repeated values, NULLs, update replacement, delete and
row compaction, columnar append, columnar reads, and integer histograms. The
string dictionary regression test remains in the same package. Verification
used:

```text
make codex-chu55-test
make codex-chu55-sql
make codex-chu55-race
make codex-chu55-vet
```

The feature only changes in-memory typed-table representation. It introduces no
new wire format, persistence format, network listener, or implicit background
activity.
