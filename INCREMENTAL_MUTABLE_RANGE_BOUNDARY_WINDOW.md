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
Same-partition, same-order update batches validate all replacements and scan
the ordered frame once. Mixed operations, duplicate keys, cross-partition
batches, and any update that changes partition or order retain the existing
affected-partition rebuild path.

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

## Batched same-position updates

Command: `make benchmark-m065ac-mutable-range-boundary-batch`.

This compares a two-row same-position update batch on one 2,000-row partition.
The baseline is the pre-change M065x implementation, which rebuilt the
affected partition for the batch. Five samples used `-benchtime=100x` on
Linux/amd64.

| Kind | Full batch rebuild | Batched same-position fast path | CPU improvement | Bytes reduction | Allocation reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| `FIRST_VALUE` | 10.333 ms, 5,911,027 B, 68,113 allocs | 1.251 ms, 389,806 B, 84 allocs | 8.26x | 15.2x | 811x |
| `LAST_VALUE` | 10.247 ms, 5,901,933 B, 68,102 allocs | 1.226 ms, 387,118 B, 49 allocs | 8.36x | 15.2x | 1,390x |

Raw baseline samples (`ns/op`, `B/op`, `allocs/op`), `FIRST_VALUE` first:

```text
10332785 5911198 68113
10369115 5911011 68112
10382362 5911027 68113
10332855 5911038 68113
9926029 5911012 68112

10180679 5901933 68102
10644860 5901918 68101
10091861 5901936 68102
10247158 5901910 68100
10551622 5901990 68102
```

Raw optimized samples, `FIRST_VALUE` first:

```text
1279865 389800 83
1251013 389826 85
1219181 389792 83
1269450 389871 84
1224644 389806 84

1280319 387116 49
1224074 387117 49
1225511 387118 49
1214121 387122 49
1293819 387132 50
```
