# Incremental MIN/MAX Frame Windows

`hatSql.NewIncrementalFrameWindow` supports exact append-only `MIN(int64)` and
`MAX(int64)` maintenance for
`ROWS BETWEEN N PRECEDING AND CURRENT ROW`.

Use `IncrementalWindowFrameMinInt64` or `IncrementalWindowFrameMaxInt64` with
the same ordered, partitioned append contract as the other bounded frame
aggregates:

```go
window, err := hatSql.NewIncrementalFrameWindow(
	hatSql.IncrementalFrameWindowDefinition{
		Kind:           hatSql.IncrementalWindowFrameMinInt64,
		OutputColumn:   "rolling_min",
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
`MIN`/`MAX`; a frame containing only NULL values emits `nil`. Input rows are
cloned before the output column is added, and duplicate keys, output-column
conflicts, callback errors, and out-of-order rows are rejected atomically.

The implementation retains the frame contributions plus a fixed-size circular
monotonic candidate deque per partition. Expired candidates are discarded when
they leave the frame, and dominated candidates are removed when a new value
arrives. State is bounded to `N+1` contributions and `N+1` deque slots, and
each update is O(1) amortized. It does not retain input row maps. Arbitrary
updates, deletes, peer-aware `RANGE` frames, and non-`int64` values remain
outside this API.

## Benchmark

Run:

```text
make benchmark-m065g-incremental-extrema-frame-window
```

The benchmark compares a full recomputation of 1,025 rows with one append
after a 1,024-row seed, using 16 partitions and a
`ROWS BETWEEN 7 PRECEDING AND CURRENT ROW` frame. Five 200 ms samples ran on
Linux/amd64 with an AMD Ryzen 9 5950X. Seed setup and incremental identity-map
capacity are outside the timer; incremental unique-key tracking remains
inside the measured append path.

| Window | Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `MIN(int64)` | Full recomputation | 373,435; 360,406; 357,249; 353,957; 347,232 | 357,249 | 427,738 | 2,247 | 1x |
| `MIN(int64)` | Incremental append | 1,010; 1,003; 1,013; 1,016; 967.6 | 1,010 | 1,212 | 9 | 354x faster |
| `MAX(int64)` | Full recomputation | 360,219; 353,066; 374,646; 362,892; 382,733 | 362,892 | 427,737 | 2,247 | 1x |
| `MAX(int64)` | Incremental append | 1,004; 1,031; 999.5; 1,091; 1,024 | 1,024 | 1,242 | 9 | 354x faster |

For this workload, incremental MIN uses about 353x fewer transient bytes and
MAX uses about 344x fewer; both use 250x fewer allocations than full
recomputation. These are append-only maintenance measurements, not a claim
about arbitrary mutable windows.
