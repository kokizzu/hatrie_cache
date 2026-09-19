# M-U06 Differential Window Frames

`hatSql.DifferentialWindow` is an opt-in maintained window for signed,
weighted updates. It supports late and out-of-order rows, retractions, and
`ROWS` or `RANGE` frame bounds without changing the existing append-only
`IncrementalRowNumberLagWindow` behavior.

## Contract

- A logical record is identified by `(DifferentialRow.Key,
  DifferentialRow.Time)`.
- A positive `Diff` inserts or increases a record weight.
- A negative `Diff` retracts existing weight. Retractions never make a
  record negative; invalid batches are rejected atomically.
- Rows are sorted by time and then key inside each partition. Equal timestamps
  therefore have deterministic order, while `RANGE` frames include all rows
  whose timestamp is within the inclusive bounds.
- `ROWS` bounds are signed row offsets from the current sorted row.
- `RANGE` bounds are signed timestamp offsets from the current row. Unsigned
  timestamp underflow and overflow saturate at zero and `math.MaxUint64`.
- `FrameCount` is the sum of weights in the frame. `Value` is optional; when
  configured, `FrameSum` is the weighted sum of callback values and
  `HasFrameSum` is true.
- `MaxRows` limits distinct retained logical records, not their weights. The
  default is `DefaultDifferentialWindowMaxRows` (`1,000,000`).
- `SnapshotWithError` should be used when a value callback can fail.
  `Snapshot` preserves a compact no-error API and returns nil on callback
  failure.

The implementation keeps updates batch-atomic. It clones retained rows and
callback inputs, so callers can reuse or mutate their input maps after an
update. Returned rows and correction rows are also detached copies.

## Example

```go
window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{
	PartitionKey: func(row hatSql.SQLRow) string { return row["region"].(string) },
	Mode:         hatSql.DifferentialWindowFrameRows,
	Start:        -2,
	End:          0,
	Value: func(row hatSql.SQLRow) (float64, bool, error) {
		value, ok := row["amount"].(int64)
		return float64(value), ok, nil
	},
})
if err != nil {
	return err
}

corrections, err := window.Apply([]hatSql.DifferentialRow{
	{Key: "a", Time: 10, Diff: 1, Row: hatSql.Row{"region": "apac", "amount": int64(10)}},
	{Key: "b", Time: 11, Diff: 1, Row: hatSql.Row{"region": "apac", "amount": int64(20)}},
	{Key: "c", Time: 12, Diff: 1, Row: hatSql.Row{"region": "apac", "amount": int64(30)}},
})
if err != nil {
	return err
}

snapshot, err := window.SnapshotWithError()
```

The first apply produces corrections for the three maintained rows. The
`apac` frame counts and sums are `(1,10)`, `(2,30)`, and `(3,60)`. A later
insert at time 11 or a partial retraction of `c` emits negative old results
and positive corrected results for only the affected partition.

## Complexity and tradeoffs

Each affected partition is currently copied and sorted during an update. The
frame calculation then evaluates the value callback once per retained record,
builds weighted prefix sums, and uses binary search for `RANGE` bounds. This
reduces frame evaluation from repeated frame scans to `O(n log n)` per affected
partition, while retaining simple batch-atomic behavior and detached output.

The current design intentionally favors correctness and bounded state over a
fully incremental index. It still allocates when a partition changes, and a
large late correction can recalculate that partition. `MaxRows`, partitioning,
and upstream compaction/frontier policies should be used to bound memory.

## Verification

```sh
make test-mu06
make verify-mu06
make benchmark-mu06-baseline
make benchmark-mu06
```

The tests cover late rows, weighted updates, retractions, both frame modes,
atomic rejection, nested row detachment, callback isolation, minimum-int64
retractions, and callback errors.

## Benchmark results

Commands use `-count=5 -benchtime=100ms` on Linux/amd64 with an AMD Ryzen 9
5950X. The M-U06 fixture applies 256 rows or alternates a late insert and
retraction against a 256-row state. The `pre-prefix` numbers are the first
correct implementation before prefix sums; `final` is the checked-in path.

| Workload | Pre-prefix ns/op samples | Final ns/op samples | Median CPU improvement | Pre B/op | Final B/op | Memory improvement | Pre allocs/op | Final allocs/op | Allocation improvement |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ROWS apply | 856358; 900937; 951340; 916222; 885629 | 471189; 436073; 454480; 466488; 446999 | 1.98x | 1,267,881 | 596,450 | 2.13x lower | 5,610 | 1,588 | 3.53x lower |
| RANGE apply | 1,017,329; 932555; 1,093,635; 1,053,772; 998519 | 463394; 434116; 473457; 470117; 445892 | 2.15x | 1,267,881 | 596,450 | 2.13x lower | 5,610 | 1,588 | 3.53x lower |
| ROWS late correction | 1,864,441; 1,755,531; 1,814,720; 1,844,737; 1,853,600 | 826107; 845889; 811292; 835793; 788659 | 2.23x | 2,254,916 | 909,371 | 2.48x lower | 11,659 | 3,599 | 3.24x lower |
| RANGE late correction | 1,931,160; 1,921,478; 1,798,472; 1,809,873; 1,815,098 | 719246; 835749; 863878; 864452; 835871 | 2.17x | 2,258,181 | 909,275 | 2.48x lower | 11,673 | 3,593 | 3.25x lower |

The existing append-only control is intentionally not treated as a direct
speed comparison because it has different semantics and a different fixture:

| Existing control | ns/op samples | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Incremental row-number/lag, lag 0 | 650200; 621817; 697679; 683787; 674814 | 674814 | 893,039 | 4,102 |
| Incremental row-number/lag, lag 2 | 970445; 997254; 956478; 916310; 945431 | 956478 | 1,248,473 | 6,245 |

See [BENCHMARK.md](BENCHMARK.md#m-u06-differential-window-frames) for the
same result in the repository-wide benchmark index.
