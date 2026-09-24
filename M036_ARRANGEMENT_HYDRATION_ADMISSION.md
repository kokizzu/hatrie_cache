# M-U36 Arrangement Hydration Admission

Typed aggregate and join arrangements can now expose bounded hydration
progress and an explicit query-admission wait without changing their existing
replay behavior.

## Contract

`TypedTableAggregateArrangement.HydrationStatus` reports its applied
checkpoint, current source sequence, remaining retained changes, generation,
and one of `ready`, `stale`, or `failed`. Join arrangements report the same
fields independently for their left and right inputs.

`WaitForHydration(ctx)` is the opt-in admission boundary. It returns only when
the arrangement is current at one status read, or returns context cancellation
or `ErrTypedTableArrangementHydrationFailed`. It does not read source rows and
does not start a worker. A caller-owned hydration worker continues to call the
existing bounded `Hydrate(limit)` method.

```go
status, err := arrangement.HydrationStatus()
if err != nil {
    return err
}
if !status.Ready {
    // A caller-owned worker may run Hydrate(1024) in another goroutine.
    status, err = arrangement.WaitForHydration(ctx)
    if err != nil {
        return err
    }
}
useArrangement(status)
```

Every successful `Hydrate` wakes waiters and advances the generation. A failed
batch records a failed status and wakes waiters; a later retry clears the
failure before using the same retained changefeed. Existing `Rows`, `Freshness`,
and `Hydrate` callers retain their prior behavior, and `Rows` is not silently
changed to block.

## Restart and source retention

The status is process-local and intentionally not a second source of truth.
`Hydrate` still applies retained `TypedTable` changes and returns
`ErrTypedTableChangesCompacted` when the source has no replay history. The
caller must restore the arrangement checkpoint or rebuild it when retention is
insufficient. Status reads compare in-memory sequence counters only, so they do
not reread source rows.

## Measured cost

Five benchmark samples on Linux/amd64 with an AMD Ryzen 9 5950X measured the
ready path:

| Operation | Median ns/op | B/op | Allocs/op | Relative to existing freshness |
| --- | ---: | ---: | ---: | ---: |
| Aggregate `Freshness` | 13.82 | 0 | 0 | 1.00x |
| Aggregate `HydrationStatus` | 35.57 | 0 | 0 | 2.57x |
| Aggregate `WaitForHydration` | 37.63 | 0 | 0 | 2.72x |
| Join `Freshness` | 25.21 | 0 | 0 | 1.00x |
| Join `HydrationStatus` | 40.49 | 0 | 0 | 1.61x |

The extra CPU is the cost of returning readiness state and remaining work for
one or two sources. It does not add heap allocation and is paid only when the
caller explicitly requests status or admission. Ordinary arrangement use is
unchanged.
