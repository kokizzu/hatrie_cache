# T-U13 Durable Cluster Membership

Status: partially adopted as an importable `hatTopology` control-plane
primitive. The default topology and replication paths are unchanged.

## Problem

The existing topology file is an atomic snapshot, and topology commits can
perform compare-and-swap and quorum admission checks. There was no durable
join/leave state machine tying membership changes to a monotone generation,
fencing token, idempotency key, and replay-verifiable history. A restarted
operator process could therefore lose the membership transition context even
when the latest topology file was present.

## API

`OpenDurableMembershipStore` creates or reopens a JSON membership file. The
initial topology is normalized and stored on first creation. On reopen, callers
may pass a zero topology, or the exact original initial topology for an
additional mismatch check.

```go
initial := hatTopology.SingleNodeTopology("node-a", "127.0.0.1:9001")
store, err := hatTopology.OpenDurableMembershipStore(
    "data/membership.json",
    initial,
)
if err != nil {
    return err
}

snapshot := store.Snapshot()
result, err := store.Apply(hatTopology.MembershipChange{
    ID:                 "join-node-b-2026-10-02",
    Kind:               hatTopology.MembershipJoin,
    ExpectedGeneration: snapshot.Generation,
    FencingToken:       snapshot.FencingToken + 1,
    Node: hatTopology.TopologyNode{
        ID:      "node-b",
        Address: "127.0.0.1:9002",
        Role:    "replica",
    },
})
if err != nil {
    return err
}
_ = result.Snapshot
```

Leave a node by ID. A node that is the local `Self` node or is referenced by a
shard primary/replica is rejected; reassign ownership first.

```go
snapshot := store.Snapshot()
_, err := store.Apply(hatTopology.MembershipChange{
    ID:                 "leave-node-b-2026-10-02",
    Kind:               hatTopology.MembershipLeave,
    ExpectedGeneration: snapshot.Generation,
    FencingToken:       snapshot.FencingToken + 1,
    NodeID:             "node-b",
})
```

`Snapshot` returns owned copies of the current topology and last record.
`History` returns owned records in generation order. `MembershipChange.ID` is
the durable idempotency key: retrying the same exact change returns
`MembershipApplyResult.Replayed` without advancing generation; reusing the ID
for a different change returns `ErrMembershipChangeConflict`.

## Durability and recovery

Every successful change is written as a complete JSON document containing the
initial topology and the membership records. The write uses the existing
`hatTopology` atomic file writer: a private temporary file is encoded, flushed,
fsynced, renamed, and the containing directory is synced before the new
in-memory state is published. A failed durable write therefore does not publish
the change in the process.

On startup, reopening the file validates:

- the file version and strict JSON shape;
- unique change IDs and contiguous generations;
- strictly increasing non-zero fencing tokens;
- each previous topology fingerprint;
- the resulting topology fingerprint for every record.

Back up `membership.json` with the topology/cache backup that it describes. To
restore, stop the writer, restore the file to the configured path, and reopen
the store. Do not hand-edit records or restore a membership file without its
matching initial topology and data snapshot. The store does not transfer files,
discover peers, authenticate operators, or run a distributed consensus
protocol.

## Caller-owned consensus boundary

The store is process-safe, not a distributed consensus authority. A caller
should collect authenticated votes with the existing topology consensus/failover
helpers, then call `Apply` with the accepted generation and a newer fencing
token. HTTP/gRPC monitoring and automatic failover do not call this API unless a
caller explicitly wires them in.

## Tradeoffs

- Membership changes are intentionally slow control-plane operations because
  each successful change is fsynced before publication.
- The current format rewrites the complete bounded JSON history for every
  change. Membership is expected to change rarely; journal compaction and
  snapshot truncation remain future work if churn makes the history large.
- No automatic shard migration is attempted. This avoids silently dropping or
  duplicating data; ownership must be changed and verified before a referenced
  node can leave.
- The existing `SaveTopology`, election, replication, and cache data paths keep
  their behavior and defaults.

See [BENCHMARK.md#t-u13-durable-cluster-membership](BENCHMARK.md#t-u13-durable-cluster-membership)
for raw samples and the measured control-plane cost.
