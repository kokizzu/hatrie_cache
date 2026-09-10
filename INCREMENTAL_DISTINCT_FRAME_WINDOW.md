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
Incrementing or decrementing a value is O(1) average. Because `Append`
preserves atomic failure semantics, its transactional partition snapshot
copies the bounded contribution state and distinct map; the benchmark below
measures that real public path.

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
| `COUNT(DISTINCT int64)` | Full recomputation | 1,239,711; 1,201,259; 1,233,445; 1,232,393; 1,220,935 | 1,232,393 | 9,472 | 1 | 1x |
| `COUNT(DISTINCT int64)` | Incremental append | 1,252; 1,166; 1,173; 1,168; 1,089 | 1,168 | 1,290 | 8 | 1,055x faster |

The pre-change full-scan-only control had a median of `1,236,520 ns/op`,
`9,472 B/op`, and `1 alloc/op`; the post-change control is within normal
benchmark noise. Incremental maintenance is about `1,055x` faster and uses
about `7.3x` fewer transient bytes. It performs `8x` as many allocations per
operation because the atomic snapshot and output path retain separate small
maps, but the total allocated bytes are substantially lower for this bounded
workload.
