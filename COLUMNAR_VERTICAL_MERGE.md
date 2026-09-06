# Columnar Vertical Merge

`hatSql.MergeColumnarParts` combines row-aligned columnar parts while loading
only the requested fields from each part. This is useful for wide immutable
parts when a merge, projection, or repair needs only a subset of columns.

```go
parts := []hatSql.ColumnarMergePart{
    hatSql.ColumnarBatchPart{Batch: partA},
    hatSql.ColumnarBatchPart{Batch: partB},
}
merged, err := hatSql.MergeColumnarParts(parts, []string{"id", "region"})
```

`ColumnarMergePart.LoadColumn` receives each requested field independently.
Storage implementations can use that callback to seek directly to a column
file or compressed stream. Unrequested columns are not read or decoded. The
result retains the input part order and contains only the requested fields.

## Contract

Each part must report a non-negative row count. Every requested field must be
present in every part and must return exactly that part's row count. The merge
rejects empty input, empty or duplicate field names, nil parts, missing fields,
short columns, and integer row-count overflow with an error.

`ColumnarBatchPart` adapts the in-memory `ColumnarBatch` representation. Plain
columns are copied for requested fields, and dictionary columns are decoded
only when requested. The output may dictionary-encode repeated strings after
the merge, preserving normal `ColumnarBatch.Value` behavior.

## Example

Two parts with rows `(1, ap)`, `(2, eu)` and `(3, ap)`, `(4, us)` can be merged
as follows:

```go
merged, err := hatSql.MergeColumnarParts(parts, []string{"id", "region"})
// merged.Rows == 4
// merged.Value("id", 2) == int64(3)
// merged.Value("region", 3) == "us"
```

The adapter is deliberately lower-level than SQL planning. Callers choose the
fields after planning the required projection, predicates, grouping keys, or
ordering keys. This keeps the storage read policy explicit and avoids making
unrequested wide payloads part of the merge working set.

## Verification

`hat/hatSql/columnar_vertical_merge_test.go` verifies selective field loading,
dictionary columns, part ordering, missing and short columns, invalid input,
and a repeatable allocation-reporting benchmark.
