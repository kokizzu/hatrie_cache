# TR-019 Tuple Field-Offset Cache

Hatrie Cache now has an opt-in field-offset cache for immutable
`ColumnarBatch` layouts. It is inspired by Tarantool tuple field-offset
caching: resolve a logical field to its physical representation once, then
reuse that slot for repeated row reads.

## Usage

For a manually built batch, prepare the layout after all encoding and packing
is complete:

```go
batch.PrepareFieldOffsets()
value, valid := batch.Value("score", row)
```

For a `TypedTable`, enable it together with the existing columnar layout cache:

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
	Name: "events",
	Columns: []hatSql.TypedTableColumn{
		{Name: "team", Kind: hatSql.TypedTableString, DictionaryEncoded: true},
		{Name: "score", Kind: hatSql.TypedTableInt64},
	},
	ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
		Enabled:          true,
		FieldOffsetCache: true,
	},
})
```

`FieldOffsetCache` is disabled by default. It is only useful for a reused
cached layout; setting it without `ColumnarCache.Enabled` is normalized away.
The cache is not serialized and does not change query results, storage, or
wire formats.

## Safety

The cache retains the established representation precedence: dictionary,
nullable-packed, boolean, numeric, plain, list, nested, then map. Missing
fields and malformed physical columns continue to return the same invalid
result. Packing and repeated-string encoding clear stale offsets before they
mutate a batch. A prepared batch must not have its physical maps mutated by a
caller; rebuild the offsets after any custom mutation.

## Measurement

Five-run local benchmark on a 128-row batch containing dictionary, packed,
boolean, numeric, and plain fields:

| Path | Median CPU | Memory/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Existing uncached lookup | 48.78 ns | 4 B | 0 |
| Prepared field-offset lookup | 27.43 ns | 4 B | 0 |
| One-time preparation | 670.7 ns | 824 B | 8 |

The repeated lookup path is about `1.78x` faster. The one-time retained cache
cost is bounded by the number of physical fields and is paid only when the
option is enabled. See [BENCHMARK.md](BENCHMARK.md#tr-019-tuple-field-offset-cache)
and the repeatable `make benchmark-tr019-c203` target.
