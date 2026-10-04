# M-U36 Hydration Progress And Admission

This feature adds an opt-in readiness contract for typed-table aggregate and
join arrangements. It is intended for restart recovery, readiness endpoints,
and query handlers that must not read a stale arrangement while a bounded
changefeed replay is still running.

## Default

The controller is disabled unless the caller creates and supplies a
`TypedTableArrangementHydrationAdmission`. Existing `Freshness`, `Hydrate`,
`Rows`, mutation, and query paths are unchanged when no controller is used.
There is no background goroutine.

## Lifecycle

```go
admission := hatSql.NewTypedTableArrangementHydrationAdmission()

// The hydrator announces the latest source sequence it must reach.
if err := admission.Start(sourceSequence); err != nil {
	return err
}

// Publish each bounded replay batch.
if err := admission.Advance(checkpoint, sourceSequence, uint64(applied)); err != nil {
	return err
}

// Query handlers wait before reading arrangement rows.
if _, err := admission.WaitReady(ctx); err != nil {
	return err
}
```

`Start` is idempotent for the active generation and extends its target when a
new source tail is observed. `Advance` rejects decreasing checkpoints or
targets. A failed generation must be explicitly `Reset` and started again;
this prevents an interrupted recovery from accidentally admitting stale rows.
`Close` permanently wakes and rejects waiters.

The progress snapshot contains the lifecycle state, generation, checkpoint,
target, cumulative changes applied in the generation, pending sequence units,
and a bounded error string. Join arrangements report the sum of their two
input sequence streams as progress units; the join's own `Complete` field
remains the authoritative per-input result.

## Arrangement Helpers

The aggregate and join leases expose an opt-in convenience method:

```go
report, err := arrangement.HydrateWithAdmission(ctx, admission, 1024)
```

The helper announces the current source tail, performs one existing bounded
`Hydrate` call, and publishes the resulting checkpoint. A nil controller takes
the exact direct `Hydrate` path. Context cancellation is checked before a
helper starts; the underlying existing hydration call remains synchronous and
bounded by its batch limit.

## Operational Pattern

1. Restore the arrangement checkpoint or create the arrangement lease.
2. Create one admission controller per shared arrangement generation.
3. Run bounded `HydrateWithAdmission` calls until `Progress().State` is
   `ready`.
4. Make query handlers call `WaitReady(ctx)` before reading arrangement rows.
5. Expose `Progress()` through readiness/metrics endpoints if desired.
6. On a replay error, keep readers blocked, repair or rebuild the arrangement,
   call `Reset`, then start and advance a new generation.

## Measurement

Command:

```text
make benchmark-mu036-hydration-admission
```

AMD Ryzen 9 5950X, Go benchmark count 5, zero allocations in every case:

| Path | Median ns/op | B/op | Allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: |
| Direct `Freshness` readiness probe | 12.01 | 0 | 0 | 1.00x |
| Ready-state `WaitReady` admission | 17.03 | 0 | 0 | 1.42x time |
| Direct no-op `Hydrate` | 27.35 | 0 | 0 | 1.00x |
| `HydrateWithAdmission` no-op batch | 62.68 | 0 | 0 | 2.29x time |

The ready-state atomic fast path is the low-cost reader path. The hydration
wrapper costs more because it performs source freshness, generation, and
progress publication around each batch. Use the wrapper at recovery/batch
boundaries and use `WaitReady` for readers; do not add it to a per-row loop.
The default disabled path has no controller cost.
