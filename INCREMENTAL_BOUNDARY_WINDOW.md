# Incremental Boundary Windows

`hatSql.NewIncrementalBoundaryWindow` maintains append-only SQL boundary
window values for the frame
`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

Supported kinds:

- `IncrementalWindowFirstValue` for `FIRST_VALUE`, including a first value of
  SQL `NULL`.
- `IncrementalWindowLastValue` for `LAST_VALUE`, which is the current row's
  value for this frame.

Rows must arrive in order within each partition. Set `Descending` when the
input order is descending. `PartitionKey` is optional; omitting it creates one
partition. `RowKey` must return a stable unique key, `ValueKey` must return the
window value or `nil`, and the output column may not already exist in an input
row. `Append` returns one positive `hatSql.DifferentialRow` per accepted input
row and never mutates input maps.

```go
window, err := hatSql.NewIncrementalBoundaryWindow(
	hatSql.IncrementalBoundaryWindowDefinition{
		Kind:         hatSql.IncrementalWindowFirstValue,
		OutputColumn: "first_name",
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
			return row["name"], nil
		},
	})
if err != nil {
	panic(err)
}

updates, err := window.Append([]hatSql.Row{
	{"id": "a", "account": "one", "sequence": int64(1), "name": "Ada"},
	{"id": "b", "account": "one", "sequence": int64(2), "name": "Lin"},
})
```

The output values for `a` and `b` are both `Ada`. A `nil` first value remains
`nil` for every later row in that partition; this API uses SQL's default
`RESPECT NULLS` behavior. `LAST_VALUE` emits each current row's value,
including `nil`.

Validation and callback execution are atomic. Callback errors, duplicate keys,
output-column conflicts, and out-of-order rows leave the window unchanged.
The maintainer retains one value per partition for `FIRST_VALUE` and no input
rows, so each append is O(1) steady-state and memory does not grow with the
partition history. Arbitrary updates, deletes, peer-aware `RANGE` frames, and
`IGNORE NULLS` variants remain outside this append-only API.

## Benchmark

Run:

```text
make benchmark-m065e-incremental-boundary-window
```

The benchmark compares a full recomputation of 1,025 rows with one append
after a 1,024-row seed, using 16 partitions. Five 200 ms samples ran on
Linux/amd64 with an AMD Ryzen 9 5950X. Seed setup and incremental identity-map
capacity are outside the timer; incremental unique-key tracking remains
inside the measured append path.

| Window | Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `FIRST_VALUE` | Full recomputation | 315,217; 305,326; 311,791; 324,647; 305,445 | 311,791 | 394,746 | 2,054 | 1x |
| `FIRST_VALUE` | Incremental append | 800.7; 818.6; 833.2; 822.1; 806.8 | 818.6 | 564 | 6 | 381x faster |
| `LAST_VALUE` | Full recomputation | 291,606; 286,751; 284,244; 294,677; 283,740 | 286,751 | 394,745 | 2,054 | 1x |
| `LAST_VALUE` | Incremental append | 795.4; 837.6; 830.5; 776.8; 820.9 | 820.9 | 561 | 6 | 349x faster |

For this workload, incremental `FIRST_VALUE` uses about 700x fewer transient
bytes and 342x fewer allocations. Incremental `LAST_VALUE` uses about 704x
fewer transient bytes and 342x fewer allocations. These are append-only
maintenance measurements, not a claim about arbitrary mutable windows.
