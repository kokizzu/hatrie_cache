# T-U21 Versioned Space Migration

`hatSchema.SpaceMigrationManager` is an opt-in control-plane state machine for
long-running, versioned space changes. It is not constructed by the normal
schema or query paths, so the default runtime behavior and the existing
`Migration` API are unchanged.

## What It Provides

- Named plans with sequential, bounded steps.
- Explicit compatibility versions for mixed-version readers and writers.
- Pause and resume after a callback error or context cancellation.
- Reverse-order rollback for completed steps.
- Deterministic, CRC32C-protected binary snapshots.
- Copy-safe status values and bounded input, error, and snapshot sizes.

The manager coordinates state and validation. The caller owns the actual data
conversion, locking, request routing, and durable snapshot destination.

## Basic Flow

```go
manager, err := hatSchema.NewSpaceMigrationManager(hatSchema.SpaceMigrationManagerOptions{})
if err != nil {
    return err
}

_, err = manager.Prepare(hatSchema.SpaceMigrationPlan{
    ID:                  "orders-v3",
    Space:               "orders",
    FromVersion:         1,
    CompatibleVersions: []uint64{1, 2},
    Steps: []hatSchema.SpaceMigrationStep{
        {ID: "add-status", TargetVersion: 2},
        {ID: "add-region", TargetVersion: 3},
    },
})
if err != nil {
    return err
}

err = manager.Run(ctx, "orders-v3", hatSchema.SpaceMigrationCallbacks{
    Check: func(ctx context.Context, plan hatSchema.SpaceMigrationPlan) error {
        return checkPreconditions(ctx, plan)
    },
    Apply: func(ctx context.Context, step hatSchema.SpaceMigrationStep) error {
        return applyStep(ctx, step)
    },
})
```

`Apply` is required. `Check` is optional and runs before the next step. A
callback error is returned unchanged and leaves the plan paused. Calling
`Run` again resumes at the first incomplete step.

## Version Admission

`AllowsVersion` is a helper for the caller's admission or routing layer; it
does not intercept requests automatically.

| State | Accepted versions |
| --- | --- |
| Prepared, paused, or running | Current version and the plan's compatible versions |
| Committed | Target version only |
| Rolled back | From version only |

Compatibility versions must be between `FromVersion` and the plan target. A
plan with no explicit compatibility list accepts its `FromVersion` while it is
in progress.

## Rollback

Rollback requires a caller-owned `Rollback` callback. Completed steps are
visited in reverse order. If rollback is interrupted, the plan becomes paused
and a later `Rollback` call resumes from the last completed reverse step.

```go
err = manager.Rollback(ctx, "orders-v3", hatSchema.SpaceMigrationCallbacks{
    Rollback: func(ctx context.Context, step hatSchema.SpaceMigrationStep) error {
        return undoStep(ctx, step)
    },
})
```

## Snapshots And Recovery

`MarshalSnapshot` stores plan metadata and status only. It never stores Go
callbacks, closures, credentials, or executable data. The payload has a small
binary header, bounded length-prefixed strings and slices, and a CRC32C trailer.
`RestoreSpaceMigrationManager` rejects truncation, checksum failures, unknown
versions, duplicate plans, invalid step sequencing, and inconsistent status.

```go
snapshot, err := manager.MarshalSnapshot()
if err != nil {
    return err
}
// Persist snapshot atomically using the application's storage policy.

restored, err := hatSchema.RestoreSpaceMigrationManager(snapshot, options)
```

Snapshots are not available while a callback is executing. Persist them at a
pause or commit boundary. The caller must durably persist the snapshot before
advertising a new compatibility window if crash recovery requires that order.

## Defaults And Limits

The manager is default-off because constructing it and invoking callbacks are
explicit application decisions.

| Option | Default | Maximum |
| --- | ---: | ---: |
| `MaxPlans` | 64 | 4096 |
| `MaxSteps` | 64 | 1024 |
| `MaxSnapshotBytes` | 1 MiB | 16 MiB |

Plan and space identifiers are limited to 256 UTF-8 bytes. Persisted callback
errors are limited to 512 bytes. Zero-valued options select the defaults.

## Measured Cost

On Linux/amd64 with an AMD Ryzen 9 5950X, the existing `SpaceCatalog.Lookup`
baseline measured `49-56 ns/op`, `0 B/op`, and `0 allocs/op`. The opt-in manager
measured `Status` at `20.9-22.5 ns/op` and `AllowsVersion` at `16.9-17.7
ns/op`, both with zero allocations in this one-entry benchmark. The operations
are different metadata paths, so these numbers are reference points rather
than a claimed replacement speedup.

`MarshalSnapshot` measured `514-517 ns/op`, `584 B/op`, and `9 allocs/op` for a
two-step plan. A complete two-step `Run` with no-op callbacks measured
`732-738 ns/op`, `720 B/op`, and `10 allocs/op`. These costs are paid only by
callers that explicitly use the manager. See the T-U21 section in
`BENCHMARK.md` for raw samples and commands.
