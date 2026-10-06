# M-G21 Hydration Progress

Materialize-style arrangement hydration now has an additive progress and ETA
surface for typed aggregate and join arrangements.

```go
progress := hatSql.NewTypedTableArrangementHydrationProgress(nil)
for {
    report, err := arrangement.HydrateWithProgress(1024, progress)
    if err != nil {
        return err
    }
    snapshot := progress.Snapshot()
    log.Printf("hydration %d/%d pending=%d eta=%s", snapshot.Completed,
        snapshot.Total, snapshot.Pending, snapshot.ETA)
    if report.Complete {
        break
    }
}
```

`Snapshot` is safe to poll from another goroutine. `Total` and `Completed`
count retained source changes from the checkpoint observed by the first update;
`Pending` is the difference. `ETA` is zero until elapsed time and completed
work provide a rate, and is always zero after completion. A join tracker counts
both input changefeeds together.

The tracker is caller-owned and intended for one hydration operation. Passing a
nil tracker is equivalent to the existing `Hydrate` call. The default
`Hydrate` path, arrangement storage, checkpoints, and error behavior are
unchanged.

## Measurement

The benchmark was run on AMD Ryzen 9 5950X with five samples and
`-benchmem`:

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Existing `Hydrate(1)` before | 858.7 | 288 | 3 |
| Existing `Hydrate(1)` after | 857.1 | 288 | 3 |
| `HydrateWithProgress(1, tracker)` | 954.6 | 288 | 3 |

The opt-in tracker measured about `1.11x` the single-change hydration latency,
with no additional allocation or byte cost. That tradeoff is why progress is
explicit rather than enabled by default. The 10,000-change rebuild benchmark
remained about `0.91-1.02 ms`, `756-757 B/op`, and `8 allocs/op` before and
after.

Focused correctness tests, race testing, and vet cover aggregate and join
progress, completion, ETA, and concurrent snapshots.
