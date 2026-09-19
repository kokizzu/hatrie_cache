# M-U08 Temporal Join State Compaction

`hatSql.DifferentialTemporalJoin.Compact` evicts temporal join rows that can
no longer match a future counterpart. It also rebuilds the per-group key
lists, which removes stale group-index entries left by ordinary retractions.
Compaction is explicit and does not change the existing apply path until a
caller supplies input frontiers.

## Safe eviction rule

For a left row at time `t`, with inclusive maximum distance `D`, the join
retains the row until both conditions hold:

1. `leftFrontier > t`, so the left input has sealed the row's timestamp and
   cannot produce a future update or retraction for it.
2. `rightFrontier > t + D`, so no future right row can fall within the
   temporal match interval.

The right-side rule is symmetric. `t + D` saturates at `math.MaxUint64`, so
timestamp overflow cannot cause premature eviction. A row at the boundary is
retained because future input at exactly the boundary is still possible.

Frontiers are monotonic. A regressive `Compact` call returns
`ErrDifferentialTemporalJoinFrontierRegression` without changing state.

## Example

```go
join, err := hatSql.NewDifferentialTemporalJoin(hatSql.DifferentialTemporalJoinDefinition{
	MaxTimeDistance: 5,
	LeftKey:         func(row hatSql.SQLRow) string { return row["account"].(string) },
	RightKey:        func(row hatSql.SQLRow) string { return row["account"].(string) },
})
if err != nil {
	return err
}

// The source frontiers are exclusive lower bounds for future timestamps.
stats, err := join.Compact(leftFrontier, rightFrontier)
if err != nil {
	return err
}
// stats.RemovedLeft/RemovedRight and stats.RetainedLeft/RetainedRight
// expose the state change for monitoring.
```

After a row is compacted, a negative update carrying a timestamp below that
side's sealed frontier returns `ErrDifferentialTemporalJoinCompacted`. This
avoids silently accepting a retraction whose historical state is gone. The
retraction timestamp is part of the `DifferentialRow` identity contract; a
missing/unknown row at an unsealed timestamp keeps the existing negative
multiplicity error.

## Memory behavior

Compaction does not keep one tombstone per evicted key. It uses the monotonic
frontier and retraction timestamp to identify compacted history, so the
memory reclaimed from the join is not replaced by a tombstone map. Group
indexes are filtered and rebuilt after eviction, preserving the existing
first-seen order for retained keys and removing stale references.

The method is a control-plane operation over the retained maps. It is
serialized with `ApplyLeft` and `ApplyRight`; invalid frontier input is
atomic. Call it after both source frontiers have advanced far enough for the
desired retention policy. Durable snapshots should persist the two frontiers
alongside the join state if compaction decisions must survive a restart.

## Verification and benchmark

```sh
make test-mu08
make verify-mu08
make benchmark-mu08-baseline
make benchmark-mu08
```

Five `-count=5` samples use an AMD Ryzen 9 5950X and 1,024 rows per side.
The load benchmark constructs and applies both sides; the compaction
benchmark builds the state outside the timer and measures only compaction.

| Workload | ns/op samples | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Pre-M-U08 join load | 3,544,154; 3,461,558; 3,267,452; 3,518,882; 3,852,966 | 3,518,882 | 2,979,092 | 13,457 |
| Final M-U08 join load | 3,397,093; 3,248,948; 3,052,619; 3,402,305; 3,561,269 | 3,397,093 | 2,979,109 | 13,457 |
| Final compaction control path | 292,683; 335,311; 332,443; 281,573; 318,647 | 318,647 | 198,953 | 43 |

The final load path has unchanged allocation count and effectively unchanged
memory; CPU variation is within the spread of these runs. The compaction
fixture removes 1,020 of 1,024 rows per side and retains four per side, a
99.6% row-state reduction. Compaction `B/op` is temporary work allocation,
not the retained join size.
