# M226: Durable Consensus Metadata

M226 adds an opt-in `hatReplication.ShardConsensusMetadataStore` that couples
the control state needed to restart one state shard safely:

- shard identity and lease owner/fencing token;
- consensus term and one vote per term;
- committed and applied log indexes;
- source frontier;
- membership/configuration generation.

```go
store, err := hatReplication.NewShardConsensusMetadataStore("us-east-1")
if err != nil {
    return err
}
state, err := store.Commit(hatReplication.ShardConsensusMetadata{
    Shard:                   "us-east-1",
    Owner:                   "worker-a",
    FencingToken:            1, // use the token returned by ShardLeaseRegistry in production
    Term:                    7,
    VotedFor:                "worker-a",
    CommitIndex:             100,
    AppliedIndex:            96,
    Frontier:                90,
    ConfigurationGeneration: 3,
})
if err != nil {
    return err
}
_ = state
checkpoint, err := store.MarshalBinary()
```

## Safety Rules

`Commit` rejects an owner change unless the fencing token increases. It rejects
lower terms, indexes, frontiers, or configuration generations. A non-empty
`VotedFor` value cannot change inside the same term, and `AppliedIndex` cannot
exceed `CommitIndex`. `Restore` validates the full record before replacing the
current value, so a corrupt checkpoint leaves the previous state intact.

The type is durable metadata, not a complete consensus implementation. It does
not elect leaders, replicate a log, or provide distributed atomic storage. The
caller must atomically persist the binary record with the corresponding shard
state, restore it before accepting work, and obtain the fencing token from
`ShardLeaseRegistry` or another trusted ownership service.

## HCM1 Format And Benchmark

The default `HCM1` binary format is bounded, deterministic, and length-prefixed
with fixed-width counters. The benchmark used one normal metadata record on an
AMD Ryzen 9 5950X, Linux amd64, five samples per case. JSON is a control using
the same exported struct, not a supported persistence format.

| Operation | HCM1 median | JSON median | HCM1 memory | JSON memory | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: |
| Commit | 86.33 ns/op | not applicable | 0 B/op, 0 allocs | not applicable | 0 allocations |
| Marshal | 126.7 ns/op | 437.7 ns/op | 80 B/op, 1 alloc | 176 B/op, 1 alloc | 3.46x CPU, 2.20x lower B/op |
| Unmarshal | 189.1 ns/op | 2,231 ns/op | 24 B/op, 3 allocs | 336 B/op, 8 allocs | 11.8x CPU, 14.0x lower B/op, 1.67x fewer allocs |

The HCM1 record was 79 bytes versus 167 bytes for JSON, or 2.11x smaller.
Raw commands:

```text
make benchmark-m226-consensus-metadata
make measure-m226-consensus-metadata-size
```
