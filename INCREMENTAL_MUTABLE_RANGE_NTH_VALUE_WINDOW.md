# Mutable RANGE NTH_VALUE Windows

M065y adds an opt-in mutable maintainer for numeric RANGE `NTH_VALUE`.
`Position` is one-based, and a frame with fewer than `Position` rows produces
the existing `nil` result. The append-only
`NewIncrementalRangeNthValueWindow` API and its defaults are unchanged.

```go
window, err := hatSql.NewMutableIncrementalRangeNthValueWindow(
	hatSql.IncrementalRangeNthValueWindowDefinition{
		Position:       64,
		OutputColumn:   "nth_value",
		FramePreceding: 256,
		OrderKey: func(row hatSql.Row) (interface{}, error) {
			return row["order"], nil
		},
		RowKey: func(row hatSql.Row) (string, error) {
			return row["id"].(string), nil
		},
		ValueKey: func(row hatSql.Row) (interface{}, error) {
			return row["value"], nil
		},
	},
)
changes, err := window.Apply([]hatSql.IncrementalRangeNthValueWindowMutation{
	{Operation: hatSql.IncrementalRangeNthValueWindowInsert, Row: row},
	{Operation: hatSql.IncrementalRangeNthValueWindowUpdate, Key: "row-1", Row: replacement},
	{Operation: hatSql.IncrementalRangeNthValueWindowDelete, Key: "row-2"},
})
```

`Apply` validates a complete batch before publishing state and emits exact
retractions/insertions. Inserts, deletes, and order or partition moves rebuild
only affected partitions through the existing append-only evaluator. A single
same-partition, same-order update scans the sorted frame once, computes the
fixed-position value per peer group, and avoids cloning the full retained
state.

The maintainer retains base rows and current output rows, so state is O(rows)
and structural mutations are O(affected-partition rows). Planner integration,
distributed frontiers, and arbitrary late-data coordination remain
caller-owned.

## Measurement

The control fully materializes a 10,000-row RANGE result for every update. The
mutable benchmark updates one row in the same partition and order with
`NTH_VALUE(64)` and a preceding bound of 256. Each result is the median of five
`200ms` samples from `make benchmark-m065y-mutable-range-nth-value`.

| Control | Mutable | CPU improvement | Bytes reduction | Allocation reduction |
| ---: | ---: | ---: | ---: | ---: |
| 27.26 ms, 3,797,044 B, 20,039 allocs | 9.31 ms, 2,869,084 B, 60 allocs | 2.93x | 1.32x | 334.0x |

The measured fast path is favorable. Structural mutations and very small
partitions intentionally retain the rebuild path for correctness.
