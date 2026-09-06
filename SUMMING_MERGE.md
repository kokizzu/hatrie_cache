# Summing Merge

`SumSQLRows` provides an explicit ClickHouse-style `SummingMergeTree`-like
merge for a batch of `SQLRow` values.

```go
merged, err := hatSql.SumSQLRows(rows,
	func(row hatSql.SQLRow) string { return row["account"].(string) },
	[]string{"count", "amount"},
)
```

Rows with the same callback key keep the first row's output position and
non-summed fields. The named columns are added using their original built-in
numeric type. Missing or nil values contribute zero. Signed and unsigned
integer overflow, floating-point overflow, type mismatches, duplicate column
names, and nonnumeric values return errors. Output maps are shallow copies, so
the input batch is not mutated or aliased.

The key callback must be deterministic. The merge is explicit and synchronous;
it does not change normal SQL execution or start background compaction. Use it
at batch or immutable-part boundaries where merge-time reduction is desired.

The reference benchmark processes 1,024 rows with 256 duplicate keys in about
`138-140 us`, using `163,409 B/op` and `2,310 allocs/op`. Those allocations
include cloned output rows and key bookkeeping, which preserve source-row
isolation. The API intentionally favors predictable correctness over silently
wrapping numeric totals.
