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

## Incremental Collector

`NewPartitionOwnershipConsensusCollector` is the reusable form for live vote
streams. It validates the proposal once, indexes voters once, and accepts
votes with `AddVote`. The collector becomes terminal as soon as quorum is
reached or mathematically impossible. `Decision` returns the same deterministic
decision shape as the batch evaluator; `Finalize` closes a round when a vote
deadline expires, and `Reset` reuses the same proposal for another round.

The collector is safe for concurrent vote producers and stores only one state
byte per voter after construction. It is intentionally not the default
replacement for `EvaluatePartitionOwnershipConsensus`: constructing a fresh
collector for one vote round costs more than the existing batch evaluator.
Reuse it when votes arrive incrementally or when the same proposal is checked
for repeated membership rounds.

On the four-voter, three-acknowledgement fixture, the latest five-sample
benchmark measured:

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing batch metadata evaluator | 738.7 | 480 | 6 | 1.00x |
| Reused collector round | 385.3 | 160 | 4 | 1.92x faster |
| Reused collector vote loop only | 171.2 | 0 | 0 | 4.31x faster |
| Fresh collector round | 982.9 | 1,012 | 13 | 1.33x slower |

The collector's gain is therefore a steady-state/repeated-round result, not a
claim that one-shot construction is cheaper. Raw samples are recorded in
[`BENCHMARK.md`](BENCHMARK.md#incremental-partition-ownership-consensus-collector).

## Measured Cost

On an AMD Ryzen 9 5950X with four voters and five samples, the existing
fingerprint-only evaluator measured a median `432.1 ns/op`, `192 B/op`, and
`3 allocs/op`. The ownership-aware evaluator measured `652.0 ns/op`, `480
B/op`, and `6 allocs/op`: `1.51x` the CPU time, `2.5x` the bytes, and `2x` the
allocations. This is a bounded sub-microsecond control-plane cost for stronger
partition metadata validation, not a replacement for the lower-cost legacy
fingerprint-only path.
