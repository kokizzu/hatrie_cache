# M250: Temporal Join Frontier Alignment

`DifferentialTemporalJoinAligned` adds an opt-in frontier gate around
`DifferentialTemporalJoin`. It holds changes until both input frontiers have
passed the change timestamp, then applies the ready rows and compacts the
underlying join at the common frontier.

The existing `DifferentialTemporalJoin` API is unchanged and remains the
default. This wrapper is useful when two inputs advance independently and
results must not be emitted before both inputs are complete for a time range.

## API

```go
aligned, err := hatSql.NewDifferentialTemporalJoinAligned(
    hatSql.DifferentialTemporalJoinDefinition{
        MaxTimeDistance: 3,
        LeftKey:         func(row hatSql.SQLRow) string { return row["key"].(string) },
        RightKey:        func(row hatSql.SQLRow) string { return row["key"].(string) },
    },
    hatSql.DifferentialTemporalJoinAlignmentOptions{
        MaxPendingChanges: 65536,
    },
)
if err != nil {
    return err
}

// Frontiers are exclusive. A row at time 10 is released after both
// frontiers are greater than 10.
updates, err := aligned.ApplyLeft(11, []hatSql.DifferentialRow{
    {Key: "left-1", Time: 10, Diff: 1, Row: hatSql.Row{"key": "a"}},
})
_ = updates
if err != nil {
    return err
}
updates, err = aligned.ApplyRight(11, []hatSql.DifferentialRow{
    {Key: "right-1", Time: 10, Diff: 1, Row: hatSql.Row{"key": "a"}},
})
```

`MaxPendingChanges` is bounded across both inputs. Zero selects
`DefaultDifferentialTemporalJoinAlignmentMaxPendingChanges` (`65536`). A
frontier regression, late change, or pending-limit violation is rejected
without accepting that batch. `Stats()` reports both input frontiers, the
common aligned frontier, and pending row counts.

## Benchmark

The benchmark uses Go `-benchmem`, one CPU, and five samples. The direct and
aligned cases use the same one-row temporal join workload.

| Case | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Direct join | 2,044 | 1,856 | 17 | 1.00x |
| Frontier-aligned join | 2,426 | 2,192 | 24 | 1.19x |

The aligned wrapper therefore costs about 19% CPU, 18% bytes, and 41% more
allocations for this small one-row workload. That cost buys frontier safety
and bounded buffering; callers that do not need that semantic should continue
using the direct join. The default direct benchmark kept the same `2.386 MB/op`
and `28,688-28,689 allocs/op` profile against the clean parent baseline.

Raw samples are recorded in `BENCHMARK.md` and can be regenerated with
`make benchmark-m250`.
