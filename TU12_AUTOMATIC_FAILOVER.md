# T-U12 Explicit Failover Proposals

`hatTopology.ElectionStore.ProposeFailover` turns an observed unhealthy shard
primary into a deterministic, fenced `TopologyCommit`. It is an explicit
control-plane operation: it does not start a goroutine, mutate the topology,
write a file, contact peers, or promote a node by itself.

```go
proposal, err := elections.ProposeFailover(shardID, hatTopology.FailoverOptions{})
if err != nil {
	return err
}

decision, err := hatTopology.EvaluateTopologyConsensus(
	policy,
	proposal.Commit.ExpectedFingerprint,
	proposal.Commit.CandidateFingerprint(),
	votes,
)
if err != nil || !decision.Satisfied {
	return err
}
if err := hatTopology.ValidateTopologyConsensusDecision(
	decision,
	proposal.Commit.ExpectedFingerprint,
	proposal.Commit.CandidateFingerprint(),
); err != nil {
	return err
}

// Pass proposal.Commit to the deployment's topology CAS/publisher.
```

The election store must receive heartbeats or explicit offline marks. An
untracked node is conservatively treated as healthy for compatibility, so a
deployment should not call failover from an uninitialized liveness store.

The default `MinHealthyOwners` is one, meaning that at least the selected
replica must be healthy. Use a larger value when local health evidence must
cover more owners; use `EvaluateTopologyConsensus` for the authoritative
multi-node quorum. The candidate is the first healthy replica in normalized
replica order, making retries deterministic. The returned candidate topology
increments `FencingToken`, moves the old primary behind the new primary, and
keeps the old topology unchanged until the caller publishes the CAS commit.
The returned topology and healthy-owner slice are independent caller-owned
copies.

Full-replica topologies use shard `0` and update `ClusterTopology.Self`.
Sharded topologies update only the requested shard. A healthy primary, missing
candidate, insufficient healthy owners, invalid topology, and fencing-token
overflow are rejected before a proposal is returned.

## Cost

The proposal path is off the normal read/write path. On the local 3-node,
1-shard fixture, five benchmark samples measured approximately:

| Path | Median time | Bytes/op | Allocs/op |
|---|---:|---:|---:|
| Explicit proposal API | 4.18 us | 4,096 | 54 |
| Equivalent caller-side sequence | 3.99 us | 4,016 | 52 |

The approximately 4.7% and 80-byte difference is the returned independent
proposal, healthy-owner list, and fencing metadata. The API intentionally pays
that bounded cost only when an operator or control-plane loop asks for a
proposal; ordinary election reads retain their existing path.
