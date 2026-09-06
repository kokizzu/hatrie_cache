# Replacing Merge

`ReplaceSQLRows` provides an explicit ClickHouse-style
`ReplacingMergeTree`-like merge for a batch of `SQLRow` values.

```go
merged, err := hatSql.ReplaceSQLRows(rows,
	func(row hatSql.SQLRow) string { return row["id"].(string) },
	func(row hatSql.SQLRow) (uint64, error) { return row["version"].(uint64), nil },
)
```

The first occurrence of each logical key fixes its output position. A row with
a higher version replaces the current row; equal versions use the later input
row. Passing a nil version callback makes the last input row win. Returned row
maps are shallow copies, so changing a returned map cannot mutate the input
batch. Nested slices, maps, and pointers remain intentionally shallow.

The key and version callbacks are required to be deterministic. Version errors
abort the operation and no partial result is returned. Empty input returns a
nil result. The function does not alter ordinary SQL execution or enable
background merging; callers choose where a replacement merge is appropriate.

The reference benchmark processes 1,024 rows with 256 duplicate keys in about
`113-124 us`, using `164,688-164,689 B/op` and `1,544 allocs/op`. The memory cost
includes the result copies and map/index bookkeeping, which buys isolation from
source-row mutation. Use it at batch or part boundaries rather than on every
single-row write.
