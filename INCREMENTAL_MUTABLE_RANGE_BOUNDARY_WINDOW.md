# Mutable RANGE Boundary Windows

M065x adds an opt-in mutable maintainer for numeric RANGE
`FIRST_VALUE` and `LAST_VALUE`. It complements the existing append-only
`NewIncrementalRangeBoundaryWindow` API without changing its defaults.

```go
window, err := hatSql.NewMutableIncrementalRangeBoundaryWindow(
	hatSql.IncrementalRangeBoundaryWindowDefinition{
		Kind:           hatSql.IncrementalRangeFirstValue,
		OutputColumn:   "first_value",
		FramePreceding: 64,
		PartitionKey: func(row hatSql.Row) (string, error) {
			return row["partition"].(string), nil
		},
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
changes, err := window.Apply([]hatSql.IncrementalRangeBoundaryWindowMutation{
	{Operation: hatSql.IncrementalRangeBoundaryWindowInsert, Row: row},
	{Operation: hatSql.IncrementalRangeBoundaryWindowUpdate, Key: "row-1", Row: replacement},
	{Operation: hatSql.IncrementalRangeBoundaryWindowDelete, Key: "row-2"},
})
```

`Apply` validates the complete batch before publishing state. Inserts,
updates, and deletes are exact differential changes. Structural changes
(order or partition changes) rebuild only the affected partitions through the
existing append-only evaluator. A single same-partition, same-order update
uses an ordered frame scan and avoids cloning the whole retained state.

The maintainer retains base rows and current output rows, so its state is
O(rows) and structural mutations are O(affected-partition rows). SQL planner
selection, distributed frontiers, and arbitrary late-data coordination remain
caller-owned.

## Measurement

The control recomputes all 2,000 rows with a full materialized RANGE scan for
each update. The mutable benchmark updates one row in a 2,000-row partition.
Each result is the median of five `200ms` samples from
`make benchmark-m065x-mutable-range-boundary`.

| Kind | Control | Mutable | CPU improvement | Bytes reduction | Allocation reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| `FIRST_VALUE` | 2.106 ms, 781,278 B, 4,010 allocs | 1.311 ms, 386,866 B, 52 allocs | 1.61x | 2.02x | 77.1x |
| `LAST_VALUE` | 2.236 ms, 781,276 B, 4,010 allocs | 1.332 ms, 385,521 B, 34 allocs | 1.68x | 2.03x | 117.9x |

The fast path is favorable on this workload. It does not claim the same result
for structural mutations or very small partitions, which continue to use the
correctness-first rebuild path.
