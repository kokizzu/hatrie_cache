# Differential Temporal Join

`hat/hatSql` provides `DifferentialTemporalJoin` for an in-memory weighted
temporal inner equi-join. Configure one key callback per side and an inclusive
`MaxTimeDistance`:

```go
join, err := hatSql.NewDifferentialTemporalJoin(hatSql.DifferentialTemporalJoinDefinition{
	MaxTimeDistance: 5,
	LeftKey:         func(row hatSql.SQLRow) string { return row["account"].(string) },
	RightKey:        func(row hatSql.SQLRow) string { return row["account"].(string) },
})
```

Apply changes independently with `ApplyLeft` and `ApplyRight`. A pair joins
only when callback keys match and the absolute timestamp difference is no more
than the configured distance. Positive and negative input weights are
preserved through multiplication with the counterpart multiplicity. Joined
rows have `left.` and `right.` field prefixes, and their timestamp is the
later input timestamp. The pair key is an opaque length-prefixed string made
from both input keys.

Rows are retained by stable input `DifferentialRow.Key`. Positive input for a
new key stores a cloned row; negative input removes its retained multiplicity.
Changes with invalid negative multiplicity or overflowing counts/pair weights
are rejected before the batch changes state. Calls are serialized by an
internal mutex, so left and right updates may be submitted concurrently.

## Measured Cost

Benchmark command:

```text
make benchmark-sql-differential-temporal-join
```

For 256 retained left rows and 1,024 right updates producing 4,096 joined
pairs on an AMD Ryzen 9 5950X development machine:

| Path | Time | Memory | Allocations |
| --- | ---: | ---: | ---: |
| Full counterpart scan | 9.0-10.2 ms/op | ~2.390 MB/op | ~28,698 allocs/op |
| Per-key indexed scan | 2.37-2.53 ms/op | ~2.386 MB/op | ~28,686 allocs/op |

The per-key index is approximately `3.6-4.3x` faster in this workload, with
slightly lower measured memory and allocation counts. Output row construction,
validation, and state updates are included.
