# CH-031 Automatic Typed JSON Subcolumns

`hatSql.JSONSubcolumnAutoMaterializer` is an opt-in helper for sources that
repeatedly serve the same scalar JSON paths. It counts observations, promotes a
path to the existing compact `ColumnarJSONSubcolumn` representation after a
threshold, and lets the source return a `ColumnarBatch` through the existing
`ColumnarJSONSubcolumnSourceResolver` contract.

The default is deliberately conservative:

| Limit | Default |
| --- | ---: |
| `MinObservations` | 3 |
| `MaxEntries` | 64 paths |
| `MaxRows` | 1,048,576 rows per path |
| `MaxBytes` | 64 MiB of estimated typed-column payload |

The zero value of `JSONSubcolumnAutoMaterializerOptions` selects these
defaults. Constructing the materializer is opt-in; existing resolvers and
ordinary row execution are unchanged unless a source uses it.

## Source Integration

The source must provide a generation that changes whenever the source contents
change. A generation mismatch never returns an older promoted column. A source
that cannot expose a generation can call `InvalidateSource` when it replaces a
snapshot.

```go
materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(
	 hatSql.JSONSubcolumnAutoMaterializerOptions{},
)
if err != nil {
	return err
}

func (source *resolver) ResolveSQLColumnarJSONSubcolumns(
	name, key string,
	fields []string,
	paths []hatSql.ColumnarJSONSubcolumnRequest,
) (hatSql.ColumnarBatch, *hatSql.ColumnarNumericSegments, bool, error) {
	rows, err := source.ResolveSQLSource(name, key)
	if err != nil {
		return hatSql.ColumnarBatch{}, nil, false, err
	}
	batch, available, err := materializer.ResolveBatch(
		hatSql.JSONSubcolumnAutoSource{
			SourceName: name,
			SourceKey:  key,
			Generation: source.generation(name, key),
		},
		fields,
		paths,
		rows,
	)
	return batch, nil, available, err
}
```

`ResolveBatch` returns `available=false` while a path is cold, oversized, or
not a stable scalar type. The SQL engine then uses its established row path.
Missing paths and explicit JSON nulls remain distinct after promotion. Complex
JSON objects and arrays are not promoted.

For lower-level integrations, `Observe` and `Lookup` operate on one
`JSONSubcolumnAutoKey` at a time. The materializer retains only compact typed
columns and bounded metadata; it does not retain the input document slice.
`Stats` exposes counts and an estimated retained payload without exposing
source values or query text.

## Measurements

The benchmark uses 4,096 deterministic JSON documents and three samples per
case on an AMD Ryzen 9 5950X Linux `amd64` host. The cached cases reuse a
promoted column for the same generation.

| Path | Median ns/op | B/op | Allocs/op | Relative to rematerialization |
| --- | ---: | ---: | ---: | --- |
| `MaterializeJSONSubcolumn` every call | 2,944,516 | 2,589,695 | 40,955 | 1.00x |
| Cached `Observe` | 101.8 | 0 | 0 | 28,925x faster |
| Cached `Lookup` | 84.08 | 0 | 0 | 35,020x faster |
| Cached `ResolveBatch` | 446.9 | 752 | 4 | 6,589x faster |

The integer column's estimated retained typed payload is 32 KiB for 4,096
rows. The source still owns the rows used to answer the current query; the
automatic materializer does not add another copy of those raw documents.
Promotion itself costs the same one-time materialization shown in the first
row, and `ResolveBatch` creates a small transient batch wrapper on each cached
call. The limits prevent unbounded path churn, row-sized allocations, and
retained typed-column growth. This feature does not change wire format or
persist automatic columns to disk; sources should rebuild them after restart.

Raw output is recorded in [BENCHMARK.md](BENCHMARK.md#ch-031-automatic-typed-json-subcolumn-promotion).
