# Collapsing Merge

`hatSql.CollapseSQLRows` provides a deterministic CollapsingMergeTree-style
merge primitive for rows carrying a `-1` or `1` sign. Rows with the same
logical key and opposite signs cancel one unmatched pair at a time.

```go
merged, err := hatSql.CollapseSQLRows(rows,
    func(row hatSql.SQLRow) string { return row["id"].(string) },
    func(row hatSql.SQLRow) (int, error) {
        return row["sign"].(int), nil
    },
)
```

The key callback defines the cancellation identity and the sign callback must
return exactly `-1` or `1`. The implementation pairs a row with the latest
unmatched opposite-sign row for that key. Surviving rows retain their input
order, and output maps are shallow copies so output mutation does not mutate
the source batch.

Unmatched rows remain in the result. This makes incomplete cancellation
visible to the caller rather than silently dropping data. The function is a
merge primitive only: it does not delete source parts, provide transactions,
or establish a durability boundary.

## Example

Input rows with signs:

```text
(id=a, sign=1, value=insert)
(id=a, sign=-1, value=delete)
(id=b, sign=1, value=unmatched)
```

The `a` pair cancels and the result is:

```text
(id=b, sign=1, value=unmatched)
```

Invalid callbacks, invalid signs, and callback errors are returned without a
partial result. Focused coverage is in
`hat/hatSql/collapsing_merge_test.go`, including pair selection, stable order,
unmatched rows, input isolation, invalid arguments, and an allocation-reporting
benchmark.
