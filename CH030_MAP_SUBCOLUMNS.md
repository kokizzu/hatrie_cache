# ClickHouse-Style Map Key/Value Subcolumn Pruning

`hatSql` now supports an additive columnar map layout for queries that read a
small number of top-level JSON object keys. The layout stores one offset vector
and flat sorted key/value vectors instead of one Go map per row:

```go
column, err := hatSql.NewColumnarMapColumn([]map[string]interface{}{
	{"country": "SG", "large_payload": "..."},
	{"country": "US", "large_payload": "..."},
})
batch := hatSql.ColumnarBatch{
	Columns:    map[string][]interface{}{"id": ids},
	MapColumns: map[string]hatSql.ColumnarMapColumn{"doc": column},
	Rows:       len(ids),
}
```

For source implementations that can prune before constructing a batch, add
`ColumnarMapSubcolumnSourceResolver`:

```go
func (source sourceImpl) ResolveSQLColumnarMapSubcolumns(
	name, key string,
	fields []string,
	paths []hatSql.ColumnarMapSubcolumn,
) (hatSql.ColumnarBatch, *hatSql.ColumnarNumericSegments, bool, error) {
	// Return only the requested paths, for example doc + $.country.
}
```

The SQL planner automatically sends canonical paths for simple expressions such
as `JSON_VALUE(doc, '$.country')`, `JSON_QUERY(doc, '$.country')`, and
`JSON_EXISTS(doc, '$.country')`. The evaluator performs a binary search over the
selected row's keys and does not build the complete object. A present `NULL`
value remains different from a missing key.

Only one-level object-member paths are currently eligible. Array indexes,
multi-level paths, dynamic paths, full-map projections, joins, ordering,
aggregation, and other richer query shapes retain the established executor.
When the source does not implement the optional resolver, or does not return a
matching `MapColumns` entry, execution falls back without changing SQL results.

There is no configuration flag, storage migration, or wire-format change. The
optimization is selected only when the source explicitly provides the compact
layout. The layout is most useful when a source can avoid loading large unused
map values; constructing a full map column and retaining the original document
elsewhere limits the storage benefit, although lookup and query allocation
costs still improve.

## Measurement

Command:

```text
make benchmark-ch030-map-c203
```

The benchmark compares the same filtered query on 1,024 and 10,000 rows. The
baseline reparses a full 4 KiB JSON document per row. The optimized case uses a
prebuilt `ColumnarMapColumn` containing only `$.country`. Five samples were
collected on Linux amd64 with an AMD Ryzen 9 5950X.

| Rows | Baseline median | Map subcolumn median | CPU improvement | Memory improvement | Allocation improvement |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 24.02 ms, 11.41 MB, 16,843 allocs | 0.700 ms, 0.726 MB, 7,934 allocs | 34.3x faster | 15.7x less | 2.12x fewer |
| 10,000 | 233.47 ms, 111.22 MB, 164,042 allocs | 6.59 ms, 7.02 MB, 77,048 allocs | 35.4x faster | 15.9x less | 2.13x fewer |

Raw `-benchmem -count=5` samples:

```text
rows=1024/full-row ns/op:       24170918 23902982 24538657 24018977 23713356
rows=1024/full-row B/op:        11407801 11407788 11407790 11407797 11407788
rows=1024/full-row allocs/op:   16843 16843 16843 16843 16843
rows=1024/map-subcolumn ns/op:  698381 699801 707965 698531 701260
rows=1024/map-subcolumn B/op:   726017 726017 726016 726017 726017
rows=1024/map-subcolumn allocs/op: 7934 7934 7934 7934 7934

rows=10000/full-row ns/op:       224533696 224632563 233468296 237076661 243912788
rows=10000/full-row B/op:        111218304 111218307 111218307 111217188 111217185
rows=10000/full-row allocs/op:   164042 164042 164042 164041 164041
rows=10000/map-subcolumn ns/op:  6586953 6484446 6521587 6695151 6617499
rows=10000/map-subcolumn B/op:   7016849 7016849 7016848 7016849 7016849
rows=10000/map-subcolumn allocs/op: 77048 77048 77048 77048 77048
```
