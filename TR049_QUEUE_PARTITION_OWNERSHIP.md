# TR-49 Queue Partition Ownership And Online Migration

Status: implemented as an explicit, transport-neutral control-plane API.

## Purpose

`PartitionedAsyncBatcher` already provides independent local partition workers,
but it did not define which node owns a partition or how ownership changes
without a partial handoff. TR-49 adds `hatPipeline.QueuePartitionOwnership`
for explicit partition ownership and a fenced online migration protocol.

This is partitioning, not automatic sharding. It does not select nodes, move
queue data, replicate records, or run a consensus protocol. Those concerns
remain with the caller's control plane and replication layer.

## Handoff protocol

1. Create a registry with an explicit partition count and initial owners.
2. Call `BeginMigration(partition, target, sourceFence)`. The source remains
   the active owner while the target copies data and catches up.
3. Call `AcknowledgeCatchUp` after the target reaches the captured fence.
4. Call `Cutover`. The target becomes the owner and the generation advances
   atomically.
5. Call `AbortMigration` instead when the copy cannot complete. The source
   remains owner and the generation still advances, invalidating the token.

```go
ownership, err := hatPipeline.NewQueuePartitionOwnership(
    hatPipeline.QueuePartitionOwnershipOptions{
        PartitionCount: 64,
        InitialOwners:  owners,
    },
)
if err != nil {
    return err
}

migration, err := ownership.BeginMigration(12, "region-b-node-2", sourceFence)
if err != nil {
    return err
}
// Copy partition 12 and apply changes through sourceFence outside this API.
if _, err := ownership.AcknowledgeCatchUp(migration, targetSequence); err != nil {
    return err
}
_, err = ownership.Cutover(migration)
return err
```

The token checks partition, source, target, fence, and generation. Reusing an
aborted or completed token is rejected. During migration, `Owner` continues to
return the source, preventing a caller from routing new work to a target that
has not reached the source fence.

## Routing views

`Snapshot` acquires an immutable view without allocation. Refresh it at a queue
batch boundary so a batch has consistent routing even if a cutover occurs in
parallel. The safe `view.Owner(partition)` method checks bounds and assignment.
For a queue loop whose partition range was validated once, use
`view.OwnerUnchecked(partition)` to avoid repeated error checks. Invalid input
to that method is intentionally a panic, like direct slice indexing.

Old views remain valid and continue to show the old owner. This is deliberate:
callers choose the boundary at which a new generation becomes visible.

`Assignments` returns a copy for diagnostics, backup manifests, and control
plane publication. It cannot mutate the registry.

## Bounds and operational behavior

- The registry is opt-in; no existing queue or cache starts using it by
  default.
- The maximum registry size is `1 << 20` partitions.
- A nil initial owner creates an intentionally unassigned partition.
- Ownership updates use copy-on-write immutable snapshots and a mutex only for
  control-plane changes. Readers use atomic snapshot publication.
- A migration updates metadata only. The caller must define the data-copy,
  replication, backup, and failover behavior around it.

## Benchmark

Linux/amd64, AMD Ryzen 9 5950X, five samples per benchmark. Values are final
medians; `x` is the measured value divided by the static-slice baseline.

| Operation | Baseline | TR-49 | Relative | Memory |
| --- | ---: | ---: | ---: | ---: |
| Static owner lookup | 0.56 ns/op | n/a | n/a | 0 B/op, 0 allocs/op |
| Manager `Owner` lookup | 0.56 ns/op | 3.74 ns/op | 6.67x | 0 B/op, 0 allocs/op |
| Reused safe snapshot `Owner` | 0.56 ns/op | 2.38 ns/op | 4.25x | 0 B/op, 0 allocs/op |
| Reused `OwnerUnchecked` view | 0.56 ns/op | 0.62 ns/op | 1.10x | 0 B/op, 0 allocs/op |
| 64-partition migration | n/a | 3.38 us/op | n/a | 15,888 B/op, 7 allocs/op |

The important performance choice is to acquire a view once per batch and use
the unchecked accessor only after validating partition indices. The control
plane pays O(partition-count) copy-on-write work during migration, while the
hot routing loop stays within about 10% of a raw static slice.
