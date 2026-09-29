# Replica Eviction And Rejoin

`hatTopology.MembershipJournal` now accepts two explicit lifecycle operations:

- `MembershipOperationEvict` removes a known stale or failed member from the
  durable membership set.
- `MembershipOperationRejoin` adds a recovered member with its current endpoint
  and topology metadata after the caller has rebuilt its data state.

Both operations use the existing journal safety rules:

- `ExpectedGeneration` must match the current membership generation.
- `FencingToken` must be strictly greater than the current fence.
- `OperationID` makes a retried identical operation idempotent and rejects a
  conflicting reuse.
- `OpenMembershipJournal` persists the candidate snapshot atomically before
  publishing it in memory.
- The journal retains the operation in replay history, so observers can see
  the eviction and rejoin transitions.

## Recovery Sequence

1. Stop routing new work to the failed member using the deployment's traffic
   and replication controls.
2. Apply an `evict` change against the current generation and fencing token.
3. Restore the member from a compatible snapshot and replay its WAL or journal
   through the existing bootstrap path.
4. Verify the restored member's data and replication position out of band.
5. Apply a `rejoin` change with the refreshed address, role, and region fields.
6. Resume traffic only after the caller's health and replication checks pass.

The journal does not copy data, authenticate a peer, run consensus, or enable
traffic automatically. Those responsibilities stay with the existing
replication, snapshot/WAL, transport, and deployment layers.

## API Shape

```go
evicted, err := journal.Apply(hatTopology.MembershipChange{
    Operation:          hatTopology.MembershipOperationEvict,
    OperationID:        "evict-node-b-2026-09-29T10:00Z",
    ExpectedGeneration: current.Generation,
    FencingToken:       current.FencingToken + 1,
    Node:               hatTopology.TopologyNode{ID: "node-b"},
})

rejoined, err := journal.Apply(hatTopology.MembershipChange{
    Operation:          hatTopology.MembershipOperationRejoin,
    OperationID:        "rejoin-node-b-2026-09-29T10:15Z",
    ExpectedGeneration: evicted.Generation,
    FencingToken:       evicted.FencingToken + 1,
    Node: hatTopology.TopologyNode{
        ID: "node-b", Address: "b-new:8000", Role: "replica",
    },
})
```

The same node ID cannot be rejoined while it is still a member. A missing node
cannot be evicted, the last member cannot be removed, and rejected operations
do not advance generation, fencing, or the persisted file.

## Security And Operations

Use a stable, unguessable operation identity from the deployment controller and
protect the membership journal file with the same ownership and filesystem
permissions as other topology state. Do not put credentials or arbitrary
payloads in `OperationID` or `TopologyNode`. The existing bounded operation-ID,
node-count, history, and byte limits still apply.

## Measurement

The focused benchmark performs join, join, remove, add for each lifecycle and
runs five 200 ms samples on an AMD Ryzen 9 5950X, linux/amd64:

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Evict + rejoin | 3736, 3748, 3639, 3613, 3628 | 3639 | 9008 | 30 |
| Leave + join control | 3692, 3624, 3875, 3775, 3642 | 3692 | 9008 | 30 |

The new lifecycle is `1.01x` faster in this run with no allocation or memory
change. The difference is within normal benchmark noise; the important result
is that the existing join/leave path did not regress.

Verification used:

```text
make test-round4-t207-red
make test-round4-topology
make test-round4-topology-race
make vet-round4-t207
make bench-round4-t207
```
