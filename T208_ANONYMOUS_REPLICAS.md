# T208 Anonymous Replicas

T208 adds an explicit anonymous-replica mode for nodes that may receive
replication and serve ordinary reads but must not participate in quorum
decisions. This follows the useful part of Tarantool's anonymous-replica
model: a lagging or disposable observer cannot make a write quorum appear
durable and cannot be promoted as a shard primary by accident.

The feature is opt-in. Existing `primary`, `replica`, empty topology roles,
explicit `JournalWriteQuorumOptions.Voters`, and disabled quorum settings keep
their existing behavior.

## Topology Configuration

Mark the node with the `anonymous` role:

```json
{
  "version": 1,
  "mode": "full_replica",
  "self": "node-a",
  "nodes": [
    {"id": "node-a", "address": "a:8000", "role": "primary"},
    {"id": "node-b", "address": "b:8000", "role": "replica"},
    {"id": "node-anon", "address": "anon:8000", "role": "anonymous"}
  ]
}
```

`hatTopology.Normalize` accepts the role for a normal node or shard replica,
but rejects an anonymous node as a shard primary. Membership journal joins also
accept the role. `hatTopology.QuorumVoterIDs` returns sorted IDs for all
non-anonymous nodes and fails if the topology has no possible voter.

```go
voters, err := hatTopology.QuorumVoterIDs(topology)
if err != nil {
    return err
}

// Use voters for ExecuteReadQuorum, topology consensus, or the legacy
// JournalWriteQuorumOptions.Voters field.
```

The helper trims and validates node IDs without mutating the topology. It is a
caller-owned boundary: an anonymous node can still be included in ordinary
replication fan-out or non-quorum reads, but must not be passed directly to a
quorum API.

## Typed Write-Quorum Configuration

For journal write quorums, use typed members so the exclusion is encoded in
the configuration rather than maintained as a separate list:

```go
quorum, err := hatReplication.NewJournalWriteQuorum(
    hatReplication.JournalWriteQuorumOptions{
        Enabled: true,
        Members: []hatReplication.QuorumMember{
            {Node: "node-a"},
            {Node: "node-b"},
            {Node: "node-anon", Anonymous: true},
        },
    },
)
```

The coordinator derives sorted, unique voters once during construction. With
the default `Required: 0`, the strict majority is calculated from non-anonymous
members only. An acknowledgement from an anonymous node is rejected as an
unknown voter. Supplying both `Members` and `Voters` is invalid to avoid two
conflicting sources of truth. `Voters` remains available for existing callers.

Read quorum execution still accepts a node slice explicitly, so callers should
pass the result of `QuorumVoterIDs` rather than the full topology when a read
must satisfy a voter quorum.

## Operational Boundary

Anonymous status is a topology/membership property, not a security boundary.
Authenticate replication and quorum messages as usual. A node that claims a
different role must be rejected by the caller's topology generation and fence
checks before it is used. T208 does not change snapshot transfer, WAL replay,
promotion, or consensus transport.

## Measurements

The pre-change explicit three-voter setup measured a median of `96.13 ns/op`,
`96 B/op`, and `2 allocs/op`; evaluation measured `62.71 ns/op`, `0 B/op`, and
`0 allocs/op`.

The final fair comparison used three real voters plus one anonymous member:

| Path | Median | Memory | Result |
| --- | ---: | ---: | --- |
| Existing explicit voters, setup | 94.09 ns/op | 96 B/op; 2 allocs/op | Legacy control path unchanged |
| Typed members, setup | 143.0 ns/op | 96 B/op; 2 allocs/op | 1.52x slower one-time setup; no memory increase |
| Existing explicit voters, evaluate | 64.56 ns/op | 0 B/op; 0 allocs/op | Steady-state control |
| Typed members, evaluate | 63.83 ns/op | 0 B/op; 0 allocs/op | Within benchmark noise; no allocation |
| Typed member filter alone | 74.94 ns/op | 32 B/op; 1 alloc/op | One-time sorted voter extraction |

The setup tradeoff is bounded and paid once when configuration is constructed.
The measured optimization removed the unused anonymous-entry capacity and
replaced the small-list map path with bounded linear duplicate checks. It
reduced typed setup from roughly `203 ns/op`/`112 B/op` to `143 ns/op`/`96 B/op`
and filtering from roughly `128 ns/op`/`48 B/op` to `75 ns/op`/`32 B/op`.

See [BENCHMARK.md](BENCHMARK.md#t208-anonymous-replicas) for raw samples.
