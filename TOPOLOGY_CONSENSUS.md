# Topology Consensus Commit

`hatTopology.TopologyCommit` and `EvaluateTopologyConsensus` provide a
transport-neutral control-plane contract for changing partition ownership.
`hatCache.TopologyStore.ApplyConsensusCommit` installs the result only when the
proposal is bound to the current fingerprint and the decision has a valid
quorum.

## Coordinated Update

The coordinator reads the current snapshot, builds a candidate with a strictly
higher fencing token, and asks the configured voters to accept both
fingerprints:

```go
expected := store.Fingerprint()
proposal, err := hatTopology.NewTopologyCommit(expected, nextTopology)
if err != nil {
	return err
}

decision, err := hatTopology.EvaluateTopologyConsensus(
	hatTopology.TopologyConsensusPolicy{
		Voters: []string{"node-a", "node-b", "node-c"},
		// Required: 0 uses a strict majority.
	},
	expected,
	proposal.CandidateFingerprint(),
	collectVotes(proposal),
)
if err != nil {
	return err
}

result, err := store.ApplyConsensusCommit(proposal, decision)
if err != nil {
	return err
}
fmt.Println(result.Applied, result.AlreadyApplied)
```

Voters with a mismatched current or candidate fingerprint are rejected. Voter
names must be unique, and one node cannot contribute more than one vote. The
default threshold is a strict majority; `Required` can set a different value
between one and the voter count. Missing votes leave the decision unsatisfied.

## Safety Contract

- `ApplyConsensusCommit` performs a compare-and-swap against the expected
  topology fingerprint while holding the store lock.
- A changed proposal must carry a fencing token strictly higher than the
  installed token. This prevents an old owner from being reactivated by a
  stale metadata update.
- Applying the same candidate again is successful and reports
  `AlreadyApplied`, so a coordinator can retry after an uncertain response.
- A persisted `TopologyStore` writes through the existing atomic topology-file
  replacement before publishing the new in-memory snapshot.
- The decision carries its voter set and both fingerprints; applying it to a
  different proposal is rejected.

`Set` remains available for bootstrap and operator-controlled updates. It does
not claim consensus and retains its existing behavior. The new API is opt-in,
so normal cache commands, replication, and reads have no added work.

## Boundary

This package does not provide a network transport, membership protocol, vote
authentication, or a Raft/Paxos implementation. The caller must collect votes
from the intended nodes through HTTP, gRPC, or another trusted control plane,
then pass the resulting decision to `ApplyConsensusCommit`. Until that wiring
exists, C153 remains only partially implemented; this feature is the reusable
commit-admission and fencing primitive.

## Measured Cost

Linux/amd64, AMD Ryzen 9 5950X, five benchmark samples, `-benchmem`:

| Operation | Median time | Heap | Allocs |
| --- | ---: | ---: | ---: |
| Existing `TopologyStore.Set`, before | 0.817 us/op | 576 B/op | 14/op |
| Existing `TopologyStore.Set`, after | 0.861 us/op | 576 B/op | 14/op |
| `EvaluateTopologyConsensus`, three voters | 0.329 us/op | 144 B/op | 3/op |
| `TopologyStore.ApplyCommit` | 2.176 us/op | 1,272 B/op | 37/op |

The safe commit path is about 2.5x the legacy metadata replacement cost and
adds no data-plane cost because it is only called by an explicit coordinator.
That is an intentional control-plane safety tradeoff, not a throughput
optimization. The legacy path remains unchanged and the new path is not used
by default.
