# C153f Partition-Ownership Consensus Collector

`hatTopology.CollectPartitionOwnershipConsensus` is an opt-in, transport-neutral
fan-out helper for the C153 partition-ownership quorum path. A caller supplies
the HTTP, gRPC, or other control-plane fetcher; the helper supplies bounded
concurrency, canonical voter ordering, exact response-to-voter identity
binding, optional HMAC verification, per-voter failure reporting, and
quorum-triggered context cancellation.

The legacy `EvaluatePartitionOwnershipConsensus` function remains unchanged.
No background worker, monitoring server, network listener, or default routing
behavior is enabled by this feature.

```go
result, err := hatTopology.CollectPartitionOwnershipConsensus(
    ctx,
    policy,
    expectedOwnership,
    func(ctx context.Context, voter string, expected hatTopology.PartitionOwnership) (hatTopology.PartitionOwnershipConsensusVote, error) {
        return requestVoteOverGRPC(ctx, voter, expected)
    },
    hatTopology.PartitionOwnershipConsensusCollectorOptions{
        MaxConcurrent: 8,
        Authenticator: authenticator,
    },
)
if err != nil {
    return err
}
if !result.Decision.Satisfied {
    return errors.New("ownership quorum was not reached")
}
```

The fetcher must honor its context. The default concurrency is 8, callers can
set a lower bound, and values above 64 or policies above 4,096 voters are
rejected. Transport or vote-validation failures are returned in deterministic
`Failures` order and do not count toward quorum. A configured authenticator
rejects unsigned, tampered, or wrong-key votes before evaluation.

## Benchmark

Command: `make benchmark-c153f-collector`.

This compares the existing serial collection pattern with the collector over
seven voters, all required, and a fixed 50 microsecond per-voter fetch delay.
That delay models network-bound control-plane work; it is not a claim about
every local callback. Median of five samples on Linux/amd64, AMD Ryzen 9 5950X:

| Path | Median latency | Median bytes | Median allocs | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing serial collection | 7,414,287 ns/op | 2,656 B/op | 14 | 1.00x |
| Bounded concurrent collector | 1,069,293 ns/op | 11,279 B/op | 107 | **6.93x lower latency; 4.25x more bytes; 7.64x more allocations** |

The latency win is appropriate for opt-in remote quorum coordination. The
additional goroutine/channel and result bookkeeping cost is intentionally
visible; callers should keep the existing evaluator for already-collected
local votes or latency-insensitive paths.

## Raw Samples

```text
BenchmarkPartitionOwnershipConsensusCollectionBaseline-32       160  7412176 ns/op  2656 B/op  14 allocs/op
BenchmarkPartitionOwnershipConsensusCollectionBaseline-32       160  7414287 ns/op  2656 B/op  14 allocs/op
BenchmarkPartitionOwnershipConsensusCollectionBaseline-32       162  7406516 ns/op  2656 B/op  14 allocs/op
BenchmarkPartitionOwnershipConsensusCollectionBaseline-32       162  7415769 ns/op  2656 B/op  14 allocs/op
BenchmarkPartitionOwnershipConsensusCollectionBaseline-32       162  7419028 ns/op  2656 B/op  14 allocs/op
BenchmarkPartitionOwnershipConsensusCollection-32              1153  1069293 ns/op 11330 B/op 107 allocs/op
BenchmarkPartitionOwnershipConsensusCollection-32              1146  1066120 ns/op 11279 B/op 107 allocs/op
BenchmarkPartitionOwnershipConsensusCollection-32              1137  1054080 ns/op 11297 B/op 107 allocs/op
BenchmarkPartitionOwnershipConsensusCollection-32              1131  1069328 ns/op 11240 B/op 107 allocs/op
BenchmarkPartitionOwnershipConsensusCollection-32              1084  1070129 ns/op 11248 B/op 107 allocs/op
```
