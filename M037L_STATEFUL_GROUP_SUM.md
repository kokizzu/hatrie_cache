# M037L Stateful Differential Group SUM

`hatSql.IncrementalGroupSumInt64` is an importable, opt-in Materialize-style
arrangement for grouped signed differential `int64` SUM maintenance.

It keeps one compact `{count, sum, time}` entry per active group. `Apply`
accepts positive and negative multiplicities, emits exact retraction/insertion
transitions, and commits a multi-row batch only after every row validates.
Groups are removed when their multiplicity reaches zero, so a group that leaves
and re-enters within one batch starts its new sum from zero, matching
`GroupSumInt64DifferentialRows` semantics. `Snapshot` and `AllRows` return
detached, key-sorted positive rows for deterministic replay.

## Example

```go
groupSum, err := hatSql.NewIncrementalGroupSumInt64(
    func(row hatSql.SQLRow) string { return row["region"].(string) },
    func(row hatSql.SQLRow) (int64, error) { return row["amount"].(int64), nil },
)
if err != nil {
    return err
}

changes, err := groupSum.Apply([]hatSql.DifferentialRow{
    {Time: 1, Diff: 2, Row: hatSql.Row{"region": "apac", "amount": int64(10)}},
    {Time: 1, Diff: 1, Row: hatSql.Row{"region": "emea", "amount": int64(7)}},
})
// changes contains:
// apac: +1 sum=20
// emea: +1 sum=7
```

For a later retraction, send the same value with a negative diff:

```go
_, err = groupSum.Apply([]hatSql.DifferentialRow{
    {Time: 2, Diff: -1, Row: hatSql.Row{"region": "apac", "amount": int64(10)}},
})
// apac changes from sum=20 to sum=10.
```

The callback pair is required. Negative multiplicity, count overflow, sum
overflow, and callback errors return an error without partially changing the
operator. A zero sum remains visible while the group count is positive.

## Performance

The benchmark uses 256 updates across 32 groups and compares the existing
full-history rebuild after every update with stateful one-row streaming and a
single 256-row batch. Five samples are recorded in
[`M037L_BENCHMARK_RAW.txt`](M037L_BENCHMARK_RAW.txt); medians are summarized
in [`BENCHMARK.md`](BENCHMARK.md#m037l-stateful-differential-group-sum).

Stateful batch application uses a pending map to preserve atomicity. It uses
about 12.4% more transient bytes than one-row streaming but about 20.6% fewer
allocations for this workload. Both paths avoid retaining historical input
rows. The feature is not automatically selected by the SQL planner.

## Verification

The focused suite covers cross-batch inserts and retractions, zero-sum
presence, zero-to-reentry reset semantics, callback errors, count and sum
overflow, atomic failure, detached sorted snapshots, randomized reference
equivalence, and the public example. The feature script also runs focused
tests, race detection, `go vet`, package tests, benchmarks, and whitespace
checks through the repository Makefile.
