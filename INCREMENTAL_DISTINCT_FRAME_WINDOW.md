# Incremental Bounded Distinct Frame Windows

`hatSql.NewIncrementalFrameWindow` can maintain exact
`COUNT(DISTINCT int64)` values for append-only SQL-style
`ROWS BETWEEN N PRECEDING AND CURRENT ROW` frames. The implementation uses a
reference-counted per-partition hash arrangement: duplicate values share one
entry, and an entry is removed only after its last contribution leaves the
frame.

```go
window, err := hatSql.NewIncrementalFrameWindow(
	hatSql.IncrementalFrameWindowDefinition{
		Kind:           hatSql.IncrementalWindowFrameCountDistinctInt64,
		OutputColumn:   "distinct_count",
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
			return row["tag"], nil
		},
	})
if err != nil {
		panic(err)
}

updates, err := window.Append([]hatSql.Row{
	{"id": "a", "account": "one", "sequence": int64(1), "tag": int64(7)},
	{"id": "b", "account": "one", "sequence": int64(2), "tag": int64(7)},
	{"id": "c", "account": "one", "sequence": int64(3), "tag": nil},
	{"id": "d", "account": "one", "sequence": int64(4), "tag": int64(9)},
})
```

The emitted `distinct_count` values are `1`, `1`, `1`, and `2`. NULL values
are ignored, duplicates count once, and an all-NULL frame emits `int64(0)`.
Non-NULL values must be `int64`; another type returns
`ErrIncrementalFrameWindowDistinctValueInvalid`. Validation and state changes
are atomic, so a rejected batch does not reserve keys or alter the frame.

Rows must arrive in order within each partition. `RowKey` values must be
unique, `PartitionKey` is optional, and `Append` does not mutate input maps.
The public API is append-only; arbitrary updates, deletes, reordering, and
peer-aware `RANGE` frames remain outside this contract.

The retained contribution queue is bounded to `N+1` rows per partition, and
the multiplicity map has at most one entry per non-NULL value in that frame.
Incrementing or decrementing a value is O(1) average. `Append` validates all
callbacks and ordering before mutation; because the distinct transition has
no post-validation error path, it reuses the validated bounded state in place
instead of copying the contribution queue and multiplicity map. Other frame
aggregates retain their copy-on-write state path for checked arithmetic errors.

## Benchmark

Run:

```text
make benchmark-m065i-incremental-distinct-frame-window
```

The benchmark compares a full recomputation of 1,025 rows with incremental
appends after a 1,024-row seed, using 16 partitions and a
`ROWS BETWEEN 7 PRECEDING AND CURRENT ROW` frame. Five samples were run on
Linux/amd64 with an AMD Ryzen 9 5950X and a 200 ms sample window. Seed setup
and tail-row construction are outside the timer; the public incremental
append, including its transactional snapshot, is measured.

| Window | Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | --- | --- | ---: | ---: | ---: | --- |
| `COUNT(DISTINCT int64)` | Full recomputation | 1,231,197; 1,263,424; 1,235,684; 1,217,719; 1,238,137 | 1,235,684 | 9,472 | 1 | 1x |
| `COUNT(DISTINCT int64)` | Incremental append | 784.2; 773.3; 773.6; 753.6; 768.8 | 773.3 | 605 | 4 | 1,598x faster |

The pre-change full-scan-only control had a median of `1,236,520 ns/op`,
`9,472 B/op`, and `1 alloc/op`; the post-change control is within normal
benchmark noise. Incremental maintenance is about `1,598x` faster and uses
about `15.7x` fewer transient bytes. It performs `4x` as many allocations as
the full-recompute control, but the zero-copy validated-state path reduced
the original M065i incremental result from 8 allocations and 1,290 B/op to
4 allocations and 605 B/op.
