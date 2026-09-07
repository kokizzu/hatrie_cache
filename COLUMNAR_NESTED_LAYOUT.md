# Columnar Array And Nested Layout

`hatSql` supports an additive physical layout for array and nested columns.
It keeps one `uint32` offset vector per parent column and stores child values
in flat vectors, avoiding one slice header and backing allocation per parent
row.

## Array Columns

```go
tags, err := hatSql.NewColumnarListColumn([][]interface{}{
	{"go", "sql"},
	{},
	{"cache"},
})
if err != nil {
	return err
}

batch := hatSql.ColumnarBatch{
	ListColumns: map[string]hatSql.ColumnarListColumn{"tags": tags},
	Rows:        3,
}
```

The physical vectors are `Offsets == [0, 2, 2, 3]` and
`Values == ["go", "sql", "cache"]`. `ColumnarBatch.Value("tags", row)`
returns an independent logical `[]interface{}` copy, so callers cannot mutate
the flat storage through a row result. `FieldRows` reports the parent row
count.

## Nested Columns

```go
events, err := hatSql.NewColumnarNestedColumn([]map[string][]interface{}{
	{"name": {"ada", "lin"}, "score": {int64(7), int64(9)}},
	{},
})
if err != nil {
	return err
}

batch := hatSql.ColumnarBatch{
	NestedColumns: map[string]hatSql.ColumnarNestedColumn{"events": events},
	Rows:          2,
}
```

All child fields in a parent row must have the same child length. Fields
missing from a row are represented by nil entries in their flat child vector.
`ColumnarBatch.Value("events", row)` returns independent child record maps.
`Validate` rejects missing initial offsets, decreasing offsets, offsets beyond
the child vectors, mismatched child lengths, and trailing unaddressed values.

## SQL And Compatibility

`ColumnarBatch.Value` and `FieldRows` understand `ListColumns` and
`NestedColumns`; `ColumnarBatchPart` merge loading and the SQL columnar layout
cache clone them correctly. Existing `Columns` and `Dictionaries` remain
valid and take precedence if a caller supplies the same field in more than one
physical map. The JSON columnar importer still produces its existing scalar
layout, so adoption is opt-in for resolvers that can retain arrays or nested
children in physical form.

Merged parts materialize requested array/nested fields into ordinary logical
row values, preserving the existing merge API and result semantics. The
offset-based form is most useful while a part is retained and scanned.

## Benchmark

Raw `go test -benchmem -count=5` output for 4,096 parent rows with three
values per row on an AMD Ryzen 9 5950X, Linux amd64:

| Layout | Sample 1 | Sample 2 | Sample 3 | Sample 4 | Sample 5 | Median | Memory | Allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Legacy slices | 191823 ns | 181578 ns | 181822 ns | 183541 ns | 182673 ns | 182673 ns | 294912 B/op | 4097 |
| Offset + flat values | 86695 ns | 92517 ns | 91517 ns | 87306 ns | 86864 ns | 87306 ns | 215040 B/op | 2 |

The offset layout was about `2.09x` faster, used about `1.37x` less measured
allocation, and reduced allocation count by about `2,048x`. It does not
remove the interface boxing cost of generic SQL values; a typed child-vector
format remains a separate optimization opportunity.
