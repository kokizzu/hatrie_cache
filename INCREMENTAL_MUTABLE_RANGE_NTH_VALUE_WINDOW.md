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
Same-partition, same-order update batches validate all replacements and scan
the peer-aware frame once. Structural updates, cross-partition batches,
duplicate keys, and mixed operations retain the existing affected-partition
rebuild path.

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

## Batched same-position updates

Command: `make benchmark-m065ad-mutable-range-nth-value-batch`.

This compares a two-row same-position `NTH_VALUE(64)` update batch on one
2,000-row partition with a preceding bound of 256. The baseline is the parent
M065ac implementation, which rebuilt the affected partition for the batch.
Five samples used `-benchtime=100x` on Linux/amd64.

| Workload | Median CPU | Median transient memory | Median allocations | Improvement vs baseline |
| --- | ---: | ---: | ---: | ---: |
| Full affected-partition batch rebuild | 10,552,957 ns/op | 6,111,239 B/op | 70,113 allocs/op | 1.00x |
| Batched same-position NTH_VALUE fast path | 1,290,925 ns/op | 389,809 B/op | 84 allocs/op | 8.17x CPU, 15.7x lower bytes, 835x fewer allocs |

Raw baseline samples (`ns/op`, `B/op`, `allocs/op`):

```text
10743070 6111437 70112
10552957 6111223 70112
10526431 6111239 70114
10421407 6111235 70114
10604592 6111280 70113
```

Raw optimized samples:

```text
1277187 389804 84
1290925 389811 84
1344679 389826 85
1249958 389799 83
1317291 389809 84
```

The optimization is limited to all-update batches that retain one partition
and each row's order position. Fallback batches preserve the established
correctness-first rebuild behavior.
