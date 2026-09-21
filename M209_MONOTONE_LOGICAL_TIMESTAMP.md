# M209: Monotone Logical Timestamp Frontiers

M209 provides one bounded, lock-free scalar frontier for logical timestamps:
`hatDataStructure.MonotoneLogicalTimestamp`.

## Semantics

- The zero value is ready for use.
- `Current` reads the greatest accepted timestamp.
- `AtLeast` checks readiness without changing state.
- Equal advances are idempotent.
- Lower advances are rejected with
  `ErrMonotoneLogicalTimestampRegressed`.
- `AdvanceIfNewer` is the allocation-free hot path for callers that handle
  stale/equal input separately.
- `Advance` retains checked error reporting for API boundaries.

The existing `hatReplication.ChangefeedFrontier` now uses this primitive and
exposes `AtLeast`. `hatSql.SQLSourceFrontierTracker` uses the same primitive
for each source partition while preserving its existing readiness, ordering,
and error behavior. Defaults remain unchanged.

## Correctness coverage

Tests cover zero and nil receivers, equal and forward advances, regression
rejection, concurrent advancement, changefeed progress errors, and SQL
partition-frontier stability. Focused tests run across `hatDataStructure`,
`hatReplication`, and `hatSql`.

## Benchmark

Commands:

```text
make benchmark-m209-monotone-frontier
make benchmark-m209-changefeed-frontier
```

Environment: Linux amd64, AMD Ryzen 9 5950X, five samples per benchmark,
`-benchtime=2s`.

| Path | Median ns/op | B/op | allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Raw atomic increment/CAS | 2.133 | 0 | 0 | baseline |
| Shared `AdvanceIfNewer` | 2.319 | 0 | 0 | 1.09x CPU cost |
| Shared checked `Advance` | 2.722 | 0 | 0 | boundary/error path |
| Changefeed adapter before M209 | 2.302 | 0 | 0 | exact old adapter baseline |
| Changefeed adapter after M209 | 2.517 | 0 | 0 | 1.09x CPU cost |
| SQL source-frontier observe after M209 | 46.07 | 0 | 0 | allocation-free |

The shared path intentionally trades a small tight-loop CPU cost for one
consistent monotonicity implementation across read and stream frontiers. It
adds no allocations or retained memory; callers that only need a hot update
attempt can use `AdvanceIfNewer`.
