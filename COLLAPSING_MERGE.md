# Collapsing Merge

`CollapseSQLRows` provides an explicit signed-row merge inspired by
ClickHouse's `CollapsingMergeTree`.

```go
merged, err := hatSql.CollapseSQLRows(rows,
	func(row hatSql.SQLRow) string { return row["id"].(string) },
	func(row hatSql.SQLRow) (int, error) { return row["sign"].(int), nil },
)
```

The sign callback must return `+1` or `-1`. Rows with the same key and
opposite signs cancel. Cancellation uses the latest unmatched opposite row
(LIFO), which makes the result deterministic even when input signs are out of
order. Unmatched rows remain, in original input order, so incomplete
cancellation is visible to the caller. Survivor maps are shallow copies and
do not alias the input maps.

Key and sign callback errors abort the operation with no partial result. Empty
input returns nil. The merge is synchronous and explicit; it does not alter
ordinary SQL execution or start background compaction.

The reference benchmark processes 1,024 rows across 256 keys in about
`296-303 us`, using `588,194-588,195 B/op` and `3,852 allocs/op`. The cost comes
from per-key positive/negative stacks and defensive survivor copies. Use it at
batch or immutable-part boundaries rather than for individual writes.
