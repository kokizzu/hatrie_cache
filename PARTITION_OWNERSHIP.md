# Partition Ownership And Fencing

`hatTopology.PartitionOwnership` is a typed snapshot of the current owner set
for one logical partition. It binds the primary and replicas to the existing
cluster topology fingerprint and fencing token.

## Reading Ownership

Use the topology API when a key or shard must be assigned to a partition:

```go
ownership, ok := topology.OwnershipForKey("sg:user:42")
if !ok {
	return errors.New("partition is not assigned")
}

fmt.Println(ownership.ShardID, ownership.Primary, ownership.Owners())
```

For a known shard, use `OwnershipForShard`. Full-replica topologies expose
their single logical partition as shard `0`. The returned replica slice and
the `Owners` result are independent snapshots.

`TopologyStore` exposes the same operations without requiring callers to
manage a topology copy. Its lookup path reads the store's already-normalized
topology and retains no separate ownership index:

```go
ownership, ok := topologyStore.OwnershipForShard(7)
if !ok {
	return errors.New("partition is not assigned")
}
```

## Write Validation

Before a partition-scoped write, validate both the snapshot and the writer:

```go
if err := topologyStore.ValidatePartitionWrite(ownership, nodeID, fenceToken); err != nil {
	return err
}
```

Validation rejects a missing partition, changed primary or replica set, stale
topology fingerprint, stale fencing token, replica writes, and fencing tokens
that do not match the current snapshot. `IsPrimary` identifies the write
owner; `IsOwner` includes replicas for read or replication selection.

## Compatibility And Limits

- Existing topology JSON is unchanged. The topology format remains version 1,
  and legacy topology files continue to decode without ownership fields.
- The snapshot uses the existing cluster-wide `fencing_token`; it does not
  invent a second per-partition epoch or lease.
- The current command and replication paths keep their existing behavior. The
  validation API is opt-in so introducing metadata cannot reject legacy
  callers unexpectedly.
- This is a control-plane contract for partition migration, recovery, and
  future per-partition write paths. It does not implement partition split,
  merge, migration, consensus, or automatic quorum changes.

Focused coverage is in `hat/hatTopology/ownership_test.go` and
`hat/hatCache/ownership_test.go`, including legacy decoding, full-replica
routing, snapshot isolation, stale snapshots, and stale write fences.
