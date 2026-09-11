# Partition Ownership Consensus

`hatTopology.EvaluatePartitionOwnershipConsensus` adds a transport-neutral
quorum check for the complete metadata of one partition. It is the ownership-
specific companion to `EvaluateTopologyConsensus`: a vote counts only when
the shard ID, primary, replica order, topology fingerprint, and fencing token
all match the expected `PartitionOwnership`.

```go
decision, err := hatTopology.EvaluatePartitionOwnershipConsensus(
	hatTopology.TopologyConsensusPolicy{
		Voters: []string{"node-a", "node-b", "node-c"},
	},
	expectedOwnership,
	votes,
)
if err != nil {
	return err
}
if err := hatTopology.ValidatePartitionOwnershipConsensusDecision(decision, expectedOwnership); err != nil {
	return err
}
```

The default required threshold is a strict majority. Results are deterministic:
voters, acknowledgements, and rejections are ordered by node ID. Missing votes
do not count. Duplicate voters, unknown voters, malformed ownership metadata,
and duplicate decision members are rejected. The returned ownership metadata
owns its replica slice and can safely outlive the caller's input.

This API does not implement a consensus transport, leader election, vote
authentication, or automatic topology publication. Callers still collect and
authenticate votes over HTTP, gRPC, or another control-plane channel, then
apply a satisfied decision through the existing topology commit path. The
normal topology and replication paths remain unchanged unless this evaluator
is explicitly used.

## Measured Cost

On an AMD Ryzen 9 5950X with four voters and five samples, the existing
fingerprint-only evaluator measured a median `432.1 ns/op`, `192 B/op`, and
`3 allocs/op`. The ownership-aware evaluator measured `652.0 ns/op`, `480
B/op`, and `6 allocs/op`: `1.51x` the CPU time, `2.5x` the bytes, and `2x` the
allocations. This is a bounded sub-microsecond control-plane cost for stronger
partition metadata validation, not a replacement for the lower-cost legacy
fingerprint-only path.
