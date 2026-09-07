# Partial Merge Join

`hatSql.MergeSortedTypedTableJoin` is a bounded-memory inner join for two
already sorted row streams. It advances one cursor per input and emits matches
through a callback instead of building a hash index or retaining the complete
result set.

```go
left := []hatSql.TypedTableMergeJoinInput{
	{Key: "order-1", Values: []hatSql.TypedTableValue{hatSql.TypedString("eu")}},
}
right := []hatSql.TypedTableMergeJoinInput{
	{Key: "region-eu", Values: []hatSql.TypedTableValue{hatSql.TypedString("eu")}},
}

err := hatSql.MergeSortedTypedTableJoin(left, right, 0, 0,
	func(row hatSql.TypedTableJoinRow) error {
		// Consume or forward row.LeftKey, row.RightKey, row.Left, and row.Right.
		return nil
	})
```

## Contract

- Each selected field must be a valid position in every input row.
- Both inputs must be ascending by the selected field. NULL and NaN values
  are ignored for ordering and never match.
- Non-NULL key kinds must agree across both inputs. Strings, int64, float64,
  and bool values are supported; float `-0` and `+0` compare equal.
- Duplicate keys emit the complete Cartesian product in input order.
- Inputs are not modified. The emitted value slices are independent clones.
- Validation completes before the first callback. A callback error stops the
  merge immediately and is returned unchanged.

Use this path when an upstream scan, sorted index, or ordered storage layout
already supplies the required order. Use `TypedTableJoin` for unsorted or
continuously changing tables, where its live hash arrangement maintains state
between updates.

## Benchmark

Raw `go test` benchmark output on an AMD Ryzen 9 5950X, Linux amd64, with
4,096 one-to-one string matches and five samples per benchmark:

| Path | Samples (ns/op) | Median (ns/op) | Bytes/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| `MergeSortedTypedTableJoin` | 767909, 763934, 822858, 805730, 749312 | 767909 | 393216 | 8192 |
| `TypedTableJoin.Rows` | 1370852, 1360226, 1397650, 1433743, 1453508 | 1397650 | 721048 | 8196 |

On this workload the merge path is approximately **1.82x faster** and uses
approximately **1.83x fewer measured bytes per operation**. Allocations are
effectively unchanged because both paths clone one left and one right value
slice per emitted match. The result is workload-dependent: the merge path
requires sorted inputs and is not a replacement for live update maintenance.
