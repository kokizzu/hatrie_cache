# Semijoin Reduction

`hatSql.NewTypedTableJoinWithOptions` can enable semijoin reduction for a
live typed-table join:

```go
join, err := hatSql.NewTypedTableJoinWithOptions(
	left,
	right,
	hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"},
	hatSql.TypedTableJoinOptions{SemijoinReduction: true},
)
```

The same option is available through
`NewTypedTableJoinArrangementsWithOptions`. The existing constructors keep
the option disabled, preserving their previous full-row retention behavior.

## Behavior

When enabled, a row with a currently matching join key retains its complete
typed value slice. A row whose key has no counterpart retains only its source
key and join-key membership. Rows with NULL or NaN join keys are not retained,
because SQL equality can never match them. When a counterpart arrives, the
join rehydrates pending rows from the source table; when the last counterpart
is removed, matched rows are demoted back to pending metadata.

The source checkpoint is checked before rehydration. If the source table has
advanced beyond the join checkpoint, activation waits until the corresponding
changes have been applied. This prevents a pending row from observing a newer
payload out of order. `TypedTableJoin.Stats` and arrangement `Stats` expose
the current full-row and pending-key counts for diagnostics.

The result set is equivalent to the default join. `ApplyLeft`, `ApplyRight`,
checkpoint handling, duplicate keys, updates, deletes, and NULL semantics are
unchanged for callers that leave the option disabled.

## When To Use It

Use semijoin reduction when the source table already owns the authoritative
row payload and a join is sparse or its counterpart changes independently.
It reduces duplicate payload retention inside the join, but it does not
remove the source table's own storage. The tradeoff is one pending-key map
entry per unmatched row and a source lookup when that row first becomes
joinable. The default full-row arrangement remains appropriate when rows are
usually matched or source lookup latency is undesirable.

## Benchmark

Raw `go test` output on an AMD Ryzen 9 5950X, Linux amd64, using 4,096
nonmatching rows on each side and five samples per sub-benchmark:

| Constructor | Samples (ns/op) | Median (ns/op) | Bytes/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| `NewTypedTableJoin` | 5305474, 5335322, 5733377, 5517402, 5427514 | 5427514 | 6893524 | 33002 |
| `NewTypedTableJoinWithOptions` | 4758660, 5102278, 4795369, 5164999, 5092526 | 5092526 | 5849171 | 24810 |

On this sparse construction workload, the opt-in mode is approximately
**1.07x faster**, uses **1.18x fewer measured bytes**, and performs **1.33x
fewer allocations**. `Stats` reports 8,192 pending keys and zero retained
full rows for the semijoin arrangement, versus 8,192 retained full rows total
(4,096 on each side) in the default arrangement. `Bytes/op` is construction
allocation, not a direct retained-heap measurement; the retention counts make
the state reduction explicit.
