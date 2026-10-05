# M-U06 Differential Window Frames

`hatSql.DifferentialWindow` now supports an opt-in explicit frame with SQL-style
boundaries in addition to the existing signed numeric `Start` and `End`
offsets. This makes cumulative and trailing windows expressible without
sentinel integer values.

## Example

```go
window, err := hatSql.NewDifferentialWindowWithFrame(
	hatSql.DifferentialWindowOptions{Mode: hatSql.DifferentialWindowFrameRows},
	hatSql.DifferentialWindowFrame{
		Start: hatSql.DifferentialWindowFrameBound{
			Kind: hatSql.DifferentialWindowBoundUnboundedPreceding,
		},
		End: hatSql.DifferentialWindowFrameBound{
			Kind: hatSql.DifferentialWindowBoundCurrentRow,
		},
	},
)
```

Available boundary kinds are:

| Boundary | Meaning |
| --- | --- |
| `DifferentialWindowBoundOffset` | Existing signed relative offset in `Offset`. |
| `DifferentialWindowBoundUnboundedPreceding` | Start at the first retained row/time. |
| `DifferentialWindowBoundCurrentRow` | Use the current row position or timestamp. |
| `DifferentialWindowBoundUnboundedFollowing` | End at the last retained row/time. |

`ROWS` applies boundaries to ordered logical records. `RANGE` applies them to
timestamps and includes all rows sharing the current timestamp. Explicit
frames reject impossible SQL-style ordering, including an unbounded-following
start or an unbounded-preceding end. Existing callers that leave `Frame` nil
continue to use `Start`/`End` unchanged.

## Measurement

Five `go test -bench` samples on `linux/amd64`, AMD Ryzen 9 5950X. The
repository workload uses the existing 256-row differential-window benchmark;
the smaller comparison benchmark uses 64 rows to isolate frame-boundary cost.

| Workload | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing bounded ROWS, before | 430,714 | 596,452 | 1,588 | 1.00x |
| Existing bounded ROWS, after | 422,768 | 596,483 | 1,588 | 1.02x faster |
| Existing bounded RANGE, before | 440,104 | 596,452 | 1,588 | 1.00x |
| Existing bounded RANGE, after | 432,516 | 596,483 | 1,588 | 1.02x faster |
| Comparable bounded ROWS | 87,687 | 125,696 | 300 | 1.00x |
| Comparable unbounded-preceding ROWS | 88,404 | 125,920 | 303 | 0.99x |

The small post-change time/byte differences in the existing workload are
benchmark noise from the unchanged copy-and-recompute path; allocation counts
and behavior remain the same. The explicit form costs 224 B and 3 allocations
in this construction-plus-apply microbenchmark, so it is intended for
subscription/window setup rather than per-row reconstruction. The default
numeric path does not construct or evaluate an explicit frame.

This is a partial M-U06 adoption. Durable spill, frontier-driven eviction,
automatic planner integration, and SQL text parsing remain caller-owned or
future work.
