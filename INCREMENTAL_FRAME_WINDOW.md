# Incremental Bounded Frame Windows

`hatSql.NewIncrementalFrameWindow` maintains append-only SQL-style
`ROWS BETWEEN N PRECEDING AND CURRENT ROW` aggregates without rescanning the
whole input for every row.

The first supported kinds are:

- `IncrementalWindowFrameCount` for exact `COUNT(*)` semantics.
- `IncrementalWindowFrameCountDistinctInt64` for exact `COUNT(DISTINCT int64)`
  semantics.
- `IncrementalWindowFrameSumInt64` for exact `SUM(int64)` semantics.
- `IncrementalWindowFrameMinInt64` for NULL-aware `MIN(int64)` semantics.
- `IncrementalWindowFrameMaxInt64` for NULL-aware `MAX(int64)` semantics.
- `IncrementalWindowFrameAvgInt64` for NULL-aware `AVG(int64)` semantics.

Rows must arrive in order within each partition. Set `Descending` when the
input order is descending. `PartitionKey` is optional; omitting it creates one
partition. `RowKey` must return a stable unique key, and the output column may
not already exist in an input row. `Append` returns one positive
`hatSql.DifferentialRow` per accepted input row and never mutates input maps.

```go
window, err := hatSql.NewIncrementalFrameWindow(
	hatSql.IncrementalFrameWindowDefinition{
		Kind:           hatSql.IncrementalWindowFrameSumInt64,
		OutputColumn:   "rolling_sum",
		FramePreceding: 2,
		PartitionKey: func(row hatSql.Row) (string, error) {
			return row["account"].(string), nil
		},
		OrderKey: func(row hatSql.Row) (interface{}, error) {
			return row["sequence"], nil
		},
		RowKey: func(row hatSql.Row) (string, error) {
			return row["id"].(string), nil
		},
		ValueKey: func(row hatSql.Row) (interface{}, error) {
			return row["amount"], nil
		},
	})
if err != nil {
	panic(err)
}

updates, err := window.Append([]hatSql.Row{
	{"id": "a", "account": "one", "sequence": int64(1), "amount": int64(5)},
	{"id": "b", "account": "one", "sequence": int64(2), "amount": nil},
	{"id": "c", "account": "one", "sequence": int64(3), "amount": int64(3)},
})
```

The outputs for `a`, `b`, and `c` contain `5`, `5`, and `8`. A frame with no
non-NULL values emits a `nil` sum, matching SQL `SUM` NULL behavior. Non-`nil`
sum values must be `int64`; overflow returns
`ErrIncrementalFrameWindowSumOverflow`. Validation and arithmetic are atomic,
so a failed batch does not advance the window or reserve its keys.

The retained aggregate state is bounded to at most `N+1` contributions per
partition. `COUNT(*)`, `SUM(int64)`, and `AVG(int64)` updates are O(1), with
checked add/subtract for the sum and a final `float64` division for averages.
`COUNT(DISTINCT int64)` uses a reference-counted hash map, so duplicate
values are counted once and outgoing values are removed in O(1) average time.
See [INCREMENTAL_DISTINCT_FRAME_WINDOW.md](INCREMENTAL_DISTINCT_FRAME_WINDOW.md)
for its atomic snapshot and allocation measurements.
`MIN(int64)` and `MAX(int64)` use a fixed-size circular monotonic deque and
are O(1) amortized per update while ignoring SQL NULL values. See
[INCREMENTAL_EXTREMA_FRAME_WINDOW.md](INCREMENTAL_EXTREMA_FRAME_WINDOW.md)
for extrema details and
[INCREMENTAL_AVERAGE_FRAME_WINDOW.md](INCREMENTAL_AVERAGE_FRAME_WINDOW.md)
for average details and measurements. Arbitrary updates, deletes, reordering,
peer-aware `RANGE` frames, and other aggregates remain outside this append-only
API. For exact row mutations with affected-partition rebuilds, see
[INCREMENTAL_MUTABLE_FRAME_WINDOW.md](INCREMENTAL_MUTABLE_FRAME_WINDOW.md).

## Benchmark

Run:

```text
make benchmark-m065d-incremental-frame-window
```

The benchmark compares a full recomputation of 1,025 rows with one append
after a 1,024-row seed, across 16 partitions. Five 200 ms samples ran on
Linux/amd64 with an AMD Ryzen 9 5950X. Seed setup and incremental identity-map
capacity are outside the timed loop; incremental unique-key tracking remains
inside the measured append path.

| Window | Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `COUNT(*)` | Full recomputation | 319,865; 317,054; 315,459; 317,024; 325,347 | 317,054 | 427,738 | 2,247 | 1x |
| `COUNT(*)` | Incremental append | 998.5; 985.8; 1,020; 1,036; 1,047 | 1,020 | 931 | 8 | 311x faster |
| `SUM(int64)` | Full recomputation | 364,720; 359,907; 364,040; 369,391; 372,976 | 364,720 | 427,737 | 2,247 | 1x |
| `SUM(int64)` | Incremental append | 1,098; 1,048; 1,101; 992.5; 1,098 | 1,098 | 944 | 10 | 332x faster |

For this workload, incremental `COUNT(*)` uses about 459x fewer transient
bytes and 281x fewer allocations. Incremental `SUM(int64)` uses about 453x
fewer transient bytes and 225x fewer allocations. These are append-only
maintenance measurements, not a claim about arbitrary mutable windows.
