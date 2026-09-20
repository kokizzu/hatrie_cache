# T-U14 VShard Bucket Map and Migration

`hatTopology` now provides an opt-in virtual-bucket ownership map for callers
that need automatic bucket assignment and controlled movement between nodes.
The existing `ClusterTopology` routing and default full-replica behavior are
unchanged.

## What It Provides

- `NewVShardBucketMap` builds a deterministic fixed virtual-bucket map from
  node IDs and a replication factor. Node input order does not affect output.
- `RouteKey` uses xxhash and precomputed compact node indexes. The steady-state
  route path performs no allocation.
- `PlanRebalance` computes only buckets whose primary or replica set changes.
  Generation and fencing tokens must advance, so an old plan cannot replace a
  newer map.
- `NewVShardMigration` coordinates a caller-owned snapshot callback, contiguous
  WAL batches, and a fenced activation callback. The callback runs without the
  migration mutex.
- `VShardMigrationCheckpoint` has bounded `VSM1` binary encoding with CRC32C,
  and can resume completed phases after a process restart.

The API deliberately does not start transport, consensus, storage, or backup
workers. Callers use the move's bucket and ownership metadata to select the
source and target stores, then use their own durable snapshot and journal
callbacks. A callback error or an ambiguous callback context permanently
fences that migration; an operator must resolve it before retrying.

## Example

```go
current, err := hatTopology.NewVShardBucketMap(hatTopology.VShardBucketMapOptions{
    BucketCount:       256,
    ReplicationFactor: 2,
    Generation:        10,
    FencingToken:      100,
    Nodes:             []hatTopology.VShardNode{{ID: "node-a"}, {ID: "node-b"}},
})
if err != nil {
    return err
}

plan, err := current.PlanRebalance(
    []hatTopology.VShardNode{{ID: "node-a"}, {ID: "node-c"}},
    11,
    101,
)
if err != nil {
    return err
}

for _, move := range plan.Moves() {
    migration, err := hatTopology.NewVShardMigration(move,
        hatTopology.VShardMigrationOptions{
            SnapshotSequence: 1_000,
            WALLastSequence:  1_025,
        })
    if err != nil {
        return err
    }
    if err := migration.ApplySnapshot(ctx, snapshotChecksum, copySnapshot); err != nil {
        return err
    }
    if err := migration.ApplyWAL(ctx, records, applyWAL); err != nil {
        return err
    }
    if err := migration.Activate(ctx, move.Target.FencingToken, commitOwnership); err != nil {
        return err
    }
}
```

`ApplyWAL` rejects gaps, duplicates, and records beyond the declared final
sequence. Payloads are copied before the callback, so the caller can reuse its
network buffer after the call returns.

## Defaults and Bounds

The feature is opt-in. A zero bucket count uses 256 buckets, a zero replication
factor uses one owner, and zero generation/fence values start at one. Bucket
counts must be powers of two and are capped at `1<<20`; node count is capped at
1024 and replication at 16. Individual WAL payloads are capped at 16 MiB,
batches at 65,536 records, and checkpoints at 4 KiB.

## Measured Cost

The benchmark ran on the repository's AMD Ryzen 9 5950X host with five samples
through `make benchmark-tu14`. The baseline was measured from clean `master`
before the feature through `make benchmark-tu14-baseline`.

| Operation | Median ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | ---: |
| Existing `HashKeyToBucket` baseline | 42.19 | 20 | 2 | 1.00x |
| VShard `RouteKey` | 33.94 | 0 | 0 | 1.24x faster |
| Rebalance plan, 256 buckets, 4 nodes | 37,937 | 75,552 | 352 | control-plane only |

The route improvement comes from xxhash plus compact precomputed owner indexes.
The rebalance plan comparison also avoids materializing unchanged buckets; the
final measurement is 2.04x faster, with 12.5% lower allocation volume and 32.0%
fewer allocations than the first implementation. The rebalance cost is paid
only when an operator creates a new map; it is not paid on key routing or
ordinary topology paths.

Focused tests, package tests, race tests, and vet cover deterministic routing,
invalid configuration, fenced state transitions, sequence validation,
checkpoint corruption, callback isolation, and concurrent status reads.
