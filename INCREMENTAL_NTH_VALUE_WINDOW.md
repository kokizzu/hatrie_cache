# Incremental NTH_VALUE Windows

`hatSql.NewIncrementalNthValueWindow` maintains append-only SQL `NTH_VALUE`
results for the frame
`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

`Position` is one-based, matching `NTH_VALUE(expr, position)`. Rows before the
requested position emit SQL `NULL`; once the position is reached, every later
row in that partition emits the selected value. The selected value may itself
be `nil`, and remains `nil` for the rest of that partition.

Rows must arrive in order within each partition. Set `Descending` when the
input order is descending. `PartitionKey` is optional; omitting it creates one
partition. `RowKey` must return a stable unique key, `ValueKey` must return the
window value or `nil`, and the output column may not already exist in an input
row. `Append` returns one positive `hatSql.DifferentialRow` per accepted input
row and never mutates input maps.

```go
window, err := hatSql.NewIncrementalNthValueWindow(
	hatSql.IncrementalNthValueWindowDefinition{
		Position:     2,
		OutputColumn: "second_name",
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

The output for `a` is `nil` and the output for `b` is `Lin`. The maintainer
retains only a row count and the selected value per partition, so state is
O(1) per partition for a fixed position and never retains input rows.
Validation and callback execution are atomic: callback errors, duplicate keys,
output-column conflicts, and out-of-order rows leave the window unchanged.

Arbitrary updates, deletes, peer-aware `RANGE` frames, dynamic positions, and
`IGNORE NULLS` variants remain outside this append-only API.

## Benchmark

Run:

```text
make benchmark-m065f-incremental-nth-value-window
```

The benchmark compares a full recomputation of 1,025 rows with one append
after a 1,024-row seed, using 16 partitions and position 4. Five 200 ms
samples ran on Linux/amd64 with an AMD Ryzen 9 5950X. Seed setup and
incremental identity-map capacity are outside the timer; incremental
unique-key tracking remains inside the measured append path.

| Window | Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `NTH_VALUE(..., 4)` | Full recomputation | 340,627; 318,620; 312,296; 321,811; 325,298 | 321,811 | 395,682 | 2,057 | 1x |
| `NTH_VALUE(..., 4)` | Incremental append | 756.2; 771.7; 814.1; 822.8; 803.2 | 803.2 | 558 | 6 | 401x faster |

For this workload, incremental maintenance uses about 709x fewer transient
bytes and 343x fewer allocations. This is an append-only maintenance result,
not a claim about arbitrary mutable windows.
