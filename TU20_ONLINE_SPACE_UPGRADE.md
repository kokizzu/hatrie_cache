# Online Space Upgrade

`hatSchema.OnlineSpaceUpgrade` is an opt-in coordinator for converting a
space from one Go value type to another while compatible reads and writes
continue. It is a coordination primitive, not a storage engine and not an
automatic schema migration service.

## Lifecycle

1. Create a coordinator with a source version, destination version, converter,
   and storage backend.
2. During `pending`, `running`, and `ready`, `Read` checks the new space first
   and converts an old-space value when the new value is absent. `Write` calls
   the backend's `WriteDual` operation.
3. Call `RunBatch` repeatedly from a worker. Each successful batch advances a
   string cursor and a migrated count. A failed conversion or write leaves the
   checkpoint unchanged, although an idempotent backend may have received part
   of the batch.
4. Persist `Checkpoint` after each successful batch. On restart, construct a
   coordinator with the same identity and call `Restore` before resuming.
5. When the phase is `ready`, call `Cutover`. Reads use only the new space and
   writes use only `WriteNew`.
6. Before cutover, `Rollback` asks the backend to discard the destination and
   moves the coordinator to `rolled_back`. Reads then use the old space and
   writes are rejected; start a new upgrade if another attempt is needed.

The coordinator serializes batch/cutover/rollback control operations. A
read/write gate prevents cutover or rollback from racing an in-flight backend
read or write, while concurrent ordinary reads and writes can still share the
gate.

## Example

```go
upgrade, err := hatSchema.NewOnlineSpaceUpgrade(
    hatSchema.OnlineSpaceUpgradeOptions[LegacyUser, CurrentUser]{
        Space:           "users",
        PreviousVersion: 1,
        NextVersion:     2,
        BatchSize:       256,
        Convert: func(old LegacyUser) (CurrentUser, error) {
            return CurrentUser{Name: old.Name, Active: true}, nil
        },
        Backend: backend,
    },
)
if err != nil {
    return err
}

for !upgrade.Progress().Ready {
    progress, err := upgrade.RunBatch(ctx)
    if err != nil {
        return err
    }
    checkpoint := upgrade.Checkpoint()
    if err := saveCheckpoint(checkpoint); err != nil {
        return err
    }
    _ = progress
}
if err := upgrade.Cutover(ctx); err != nil {
    return err
}
```

The backend contract is deliberately explicit:

- `ReadOld` and `ReadNew` provide the two read paths.
- `ScanOld` returns ordered records, the next cursor, and a completion flag.
- `WriteDual` must be idempotent because a failed batch can be retried.
- `WriteNew` handles writes after cutover.
- `Cutover` and `Rollback` are backend-owned publication/cleanup barriers.

## Safety rules

- The default batch size is 256 records and the maximum is 4096.
- Space names, keys, and cursors have bounded byte lengths before backend calls.
- The scanner must return strictly advancing keys. Empty non-terminal batches and
  non-advancing cursors fail with `ErrOnlineSpaceUpgradeProgress`.
- The destination version must be greater than the previous positive version.
- Backend authentication, authorization, durable checkpoint storage, and
  atomicity of the backend's publication operation remain the caller's
  responsibility.
- Do not treat the checkpoint as a backup. It contains progress metadata, not
  source or destination values.

## Measured tradeoff

The benchmark compares direct map-backed conversion/dual-write work with the
same work through the coordinator and its read/write gate. It measures CPU and
allocation cost, not storage I/O or a real network backend.

| Workload | Median ns/op | B/op | allocs/op | Relative CPU |
|---|---:|---:|---:|---:|
| Direct dual-write baseline | 29.72 | 0 | 0 | 1.00x |
| Coordinator `Write` | 45.01 | 0 | 0 | 1.51x |

Raw five-run samples:

```text
Baseline: 29.55, 30.25, 29.72, 29.64, 31.07 ns/op
Coordinator: 47.08, 45.01, 43.69, 42.86, 47.87 ns/op
```

The coordinator costs about 15.29 ns per write in this in-process benchmark,
with no additional allocation or retained-memory cost. That CPU cost is the
price of the opt-in conversion, callback, phase, and cutover-safety contract;
the existing storage path remains unchanged when the feature is not used.

