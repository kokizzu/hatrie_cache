# Differential `INTERSECT ALL`

`hatSql.DifferentialIntersect` incrementally maintains the multiset
intersection of two signed differential streams. Each input side retains a
checked `uint64` multiplicity by stable row key. For every update, the output
weight is the checked signed delta of `min(leftCount, rightCount)`.

```go
operator := hatSql.NewDifferentialIntersect()
updates, err := operator.Apply(
	[]hatSql.DifferentialRow{
		{Key: "alice", Time: 1, Diff: 2, Row: hatSql.Row{"name": "Alice"}},
	},
	[]hatSql.DifferentialRow{
		{Key: "alice", Time: 1, Diff: 1},
	},
)
// updates: alice diff=1
```

`Apply` processes the left batch before the right batch, emits only non-zero
transitions, and uses the first active left-side row as the output payload.
Input row maps and byte values are cloned at the ownership boundary. A staged
validation overlay makes a failed batch atomic: missing keys, negative input
multiplicity, `uint64` count overflow, and an unrepresentable `int64` output
delta return an error without changing operator state. `Reset` drops all
retained counts and payloads.

This is a reusable Go operator for `INTERSECT ALL`-style maintenance. It does
not add parser syntax or alter the existing SQL executor. The operator is not
safe for concurrent use; synchronize calls when multiple producers share one
instance. Its retained state is proportional to the active union of keys,
which is the cost of avoiding a full relation rebuild for each update.
