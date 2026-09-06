# Replacing Merge

`hatSql.ReplaceSQLRows` provides a deterministic ReplacingMergeTree-style
utility for collapsing duplicate logical rows. The caller supplies the
logical-key function and may supply a version function.

```go
merged, err := hatSql.ReplaceSQLRows(rows,
    func(row hatSql.SQLRow) string { return row["id"].(string) },
    func(row hatSql.SQLRow) (uint64, error) {
        return row["version"].(uint64), nil
    },
)
```

With a version callback, the greatest version wins. Equal versions use the
later input row. Without a version callback, the last input row for each key
wins. The result keeps the first-seen order of logical keys, which makes output
stable across runs and avoids an implicit sort.

The function validates the key callback, propagates version errors, and
returns shallow copies of selected rows. Replacing or mutating an output map
therefore does not mutate the input row maps.

This is a merge primitive rather than an automatic storage policy. Callers can
apply it after reading immutable parts, during a compaction boundary, or in a
materialized projection where the key and version semantics are known. It does
not delete source data and does not provide a transaction or durability
boundary by itself.

## Example

Input rows:

```text
(id=a, version=1, value=old)
(id=b, version=4, value=b)
(id=a, version=3, value=new)
(id=b, version=2, value=stale)
```

The result is:

```text
(id=a, version=3, value=new)
(id=b, version=4, value=b)
```

Focused coverage is in `hat/hatSql/replacing_merge_test.go`, including stable
ordering, version ties, last-row behavior without versions, callback errors,
input isolation, invalid callbacks, and an allocation-reporting benchmark.
