# M-U36: Hydration Progress And Query Admission

Typed aggregate and join arrangements can be restored from a retained table
changefeed. Before replay finishes, their rows are not a complete view of the
source. M-U36 adds an explicit, opt-in admission contract so a caller can
inspect progress and reject a query until the arrangement is ready.

## API

```go
status, err := arrangement.HydrationStatus()
if err != nil {
    return err
}
if err := status.Admit(); err != nil {
    return err
}
```

`TypedTableArrangementHydrationStatus` reports `Checkpoint`,
`SourceSequence`, `Pending`, and `Ready`. Join arrangements expose the same
fields for both inputs through `TypedTableJoinArrangementHydrationStatus`.
`Admit` returns `ErrTypedTableArrangementNotReady` while any retained source
changes remain.

For a bounded recovery step, use:

```go
status, err := arrangement.HydrateUntilReady(ctx, 1024)
```

The method replays retained changes in the same batches as `Hydrate` and stops
when the arrangement is ready or the context is canceled. It does not wait for
future writes. A compacted changefeed still returns
`ErrTypedTableChangesCompacted`; the caller must rebuild or restore from a
newer checkpoint.

## Consistency And Cost

Status is a point-in-time snapshot. A source can receive a new change after a
successful `Admit`, so callers that need a stronger boundary must retain and
recheck their own source/version fence. No row values or query text are
included in the status.

The feature is opt-in. Existing `Apply`, `Hydrate`, and write paths do not
record additional state, start a goroutine, or change their defaults.

Measured on the local AMD Ryzen 9 5950X, Go benchmark mode, 10,000-row
changefeed fixture:

| Path | Before | After | Result |
| --- | ---: | ---: | --- |
| Existing one-change hydration | 795 ns/op, 288 B/op, 3 allocs/op | 714 ns/op, 288 B/op, 3 allocs/op | Allocation-neutral; timing difference treated as noise |
| New aggregate `HydrationStatus` | N/A | 13.13 ns/op, 0 B/op, 0 allocs/op | New zero-allocation status read |

The `HydrateUntilReady` helper performs the same replay work as an explicit
caller loop. It improves correctness and admission ergonomics, not replay
throughput.
