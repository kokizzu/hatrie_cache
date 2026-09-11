# Differential Difference

`hatSql.NegateDifferentialRows` and `hatSql.ExceptDifferentialRows` provide a
Materialize-style signed difference operator for reusable `(row, time, diff)`
streams.

`NegateDifferentialRows` flips every non-zero `Diff` in one pass. It clones row
maps and byte values, so callers can safely reuse the input batch. A zero
weight is a no-op and is omitted. Negating `math.MinInt64` is rejected instead
of wrapping.

`ExceptDifferentialRows(left, right)` computes `left - right`. It combines
updates with the same key and logical time, retains duplicate multiplicity in
the signed weight, removes zero-sum identities, and preserves first-seen
output order. The operation is exact for positive and negative updates. It
returns no partial output for missing keys, weight overflow, or an
unrepresentable negation; neither input batch is mutated.

```go
left := []hatSql.DifferentialRow{
	{Key: "alice", Time: 1, Diff: 2, Row: hatSql.Row{"team": "red"}},
	{Key: "bob", Time: 1, Diff: 1, Row: hatSql.Row{"team": "blue"}},
}
right := []hatSql.DifferentialRow{
	{Key: "alice", Time: 1, Diff: 1, Row: hatSql.Row{"team": "red"}},
}
updates, err := hatSql.ExceptDifferentialRows(left, right)
// updates: alice diff=1, bob diff=1
```

This is a reusable batch operator. It does not add SQL parser syntax or change
the existing query executor. For large persistent relations, use typed
arrangements or indexed SQL paths to avoid rebuilding arbitrary row batches.
