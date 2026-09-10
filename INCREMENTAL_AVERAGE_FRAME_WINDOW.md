# Incremental AVG Frame Windows

`hatSql.NewIncrementalFrameWindow` supports exact append-only `AVG(int64)`
maintenance for `ROWS BETWEEN N PRECEDING AND CURRENT ROW`.

Use `IncrementalWindowFrameAvgInt64` with the same ordered, partitioned append
contract as the other bounded frame aggregates:

```go
window, err := hatSql.NewIncrementalFrameWindow(
	hatSql.IncrementalFrameWindowDefinition{
		Kind:           hatSql.IncrementalWindowFrameAvgInt64,
		OutputColumn:   "rolling_average",
		FramePreceding: 2,
		OrderKey: func(row hatSql.Row) (interface{}, error) {
			return row["sequence"], nil
		},
		RowKey: func(row hatSql.Row) (string, error) {
			return row["id"].(string), nil
		},
		ValueKey: func(row hatSql.Row) (interface{}, error) {
			return row["value"], nil
		},
	})
if err != nil {
	panic(err)
}
```

`ValueKey` must return `int64` or `nil`. NULL values are ignored, matching SQL
`AVG`; a frame containing only NULL values emits `nil`. Non-NULL results are
returned as `float64`. The running sum uses the same checked `int64`
accumulator as `SUM(int64)`, so `ErrIncrementalFrameWindowSumOverflow` is
returned if the bounded frame sum cannot be represented. This avoids the
allocation and precision cost of a big-number accumulator while keeping the
common typed path compact and predictable.

Validation, callback execution, and arithmetic are atomic. Input rows are
cloned before the output column is added, and duplicate keys, output-column
conflicts, callback errors, out-of-order rows, invalid values, and overflow do
not partially advance the window.

The implementation retains at most `N+1` contributions plus a running sum and
non-NULL count per partition. Each update is O(1), including the outgoing-row
subtraction and final division. Arbitrary updates, deletes, peer-aware `RANGE`
frames, and non-`int64` values remain outside this API.

## Benchmark

Run:

```text
make benchmark-m065h-incremental-average-frame-window
```

The benchmark compares a full recomputation of 1,025 rows with one append
after a 1,024-row seed, using 16 partitions and a
`ROWS BETWEEN 7 PRECEDING AND CURRENT ROW` frame. Five 200 ms samples ran on
Linux/amd64 with an AMD Ryzen 9 5950X. Seed setup and incremental identity-map
capacity are outside the timer; incremental unique-key tracking remains
inside the measured append path.

| Window | Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | --- | --- | ---: | ---: | ---: | --- |
| `AVG(int64)` | Full recomputation | 375,757; 381,391; 388,279; 384,735; 380,631 | 381,391 | 435,932 | 3,271 | 1x |
| `AVG(int64)` | Incremental append | 1,027; 962.3; 961.9; 1,042; 1,026 | 1,026 | 1,111 | 9 | 372x faster |

For this workload, incremental AVG uses about `392x` fewer transient bytes and
`363x` fewer allocations than full recomputation. The result is an
append-only maintenance measurement, not a claim about arbitrary mutable
windows or unbounded-precision averages.
