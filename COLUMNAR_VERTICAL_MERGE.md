# Vertical Columnar Merge

`hat/hatSql.MergeColumnarParts` combines physical columnar parts while asking
each part for only the requested fields. A storage adapter can therefore pass
the primary key and changed columns without reading unchanged wide payload
columns.

```go
type Part struct {
    rows int
}

func (part Part) RowCount() int { return part.rows }

func (part Part) LoadColumn(field string) ([]interface{}, bool, error) {
    // Read only field from the part's storage.
    return loadField(field)
}

merged, err := hatSql.MergeColumnarParts(parts, []string{"id", "status"})
```

The merger validates non-negative row counts, duplicate or empty field names,
missing fields, and per-field row alignment. Part order and row order are
preserved. The built-in `ColumnarBatchPart` adapter reads either plain or
dictionary columns and exposes the same logical values. The merged result uses
the existing width-aware dictionary selector.

This API is additive and does not alter the default SQL resolver path. It is
most useful to a storage provider that can defer physical column reads until
`LoadColumn` is called.

## Measurement

Run:

```text
make benchmark-columnar-vertical-merge
```

The focused benchmark merges four 256-row parts while requesting two of three
columns. The observed development-host result was:

| Workload | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Four 256-row parts, two requested columns | 49,053 | 85,104 | 19 |

Rerun the benchmark for current-host numbers because scheduler and compiler
versions affect it.
