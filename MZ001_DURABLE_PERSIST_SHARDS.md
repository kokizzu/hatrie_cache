# MZ-001 Durable Persist Shards

This is the first importable slice of Materialize-style durable persist
shards. `hatPipeline.DurablePersistShard` owns one independently hydratable
materialized shard checkpoint. The payload is opaque, so a caller can restore
its collection state without rereading the source collection.

The shard contract contains:

- a validated shard ID;
- a monotone generation and upper frontier;
- a caller-owned materialized payload;
- versioned `HPS1` binary framing and CRC32 corruption detection;
- bounded snapshots (16 MiB default, 256 MiB maximum);
- idempotent retries for the same generation and payload;
- atomic rejection of stale, wrong-shard, corrupt, oversized, or canceled
  restores.

`FrontierSnapshotStore` is reused as the storage boundary. Existing
`NewFrontierSnapshotFileStore` therefore provides an atomic fsync-and-rename
local implementation, while object stores and replication layers can implement
the same two-method interface. `Save` and `Hydrate` are explicit and opt-in;
SQL planner/source connector wiring and distributed consensus remain caller
owned.

CRC32 detects accidental corruption but is not authentication. Payloads that
cross an untrusted boundary still need authenticated encryption or a trusted
transport.

## Example

```go
shard, err := hatPipeline.NewDurablePersistShard(hatPipeline.DurablePersistShardOptions{
    ShardID: "orders-eu",
})
if err != nil {
    return err
}
if err := shard.Publish(generation, upperFrontier, encodedRows); err != nil {
    return err
}
if err := shard.Save(ctx, store); err != nil {
    return err
}

restored, _ := hatPipeline.NewDurablePersistShard(
    hatPipeline.DurablePersistShardOptions{ShardID: "orders-eu"},
)
found, err := restored.Hydrate(ctx, store)
// found=true means encodedRows is available without a source reread.
```

## Benchmark

Commands:

```text
make benchmark-mz001-baseline
make benchmark-mz001-persist-shard
```

The fixture uses 4,096 source rows for the replay baseline and one 128 KiB
opaque payload for the durable shard path. Each result is the median of five
100 ms samples on the AMD Ryzen 9 5950X Linux/amd64 host.

| Path | Median ns/op | B/op | allocs/op | Relative to source replay |
| --- | ---: | ---: | ---: | --- |
| Source replay baseline | 252,340 | 491,888 | 4,114 | 1.00x |
| Durable shard marshal | 37,581 | 270,360 | 3 | 6.71x faster; 1.82x lower bytes; 1,371x fewer allocs |
| Durable shard hydrate | 52,913 | 401,537 | 6 | 4.77x faster; 1.23x lower bytes; 686x fewer allocs |

This is an envelope and source-replay comparison, not a claim that decoding
every application payload is free. The shard deliberately leaves payload
decoding to the embedding collection so it can use its own typed or columnar
representation. The measured tradeoff is bounded snapshot copying and CRC
work in exchange for restart hydration without source access.

Raw samples:

```text
Source replay:       304221 277303 250627 245842 252340 ns/op; 491888 B/op; 4114 allocs/op
Durable marshal:      36989  39127  37257  37581  38741 ns/op; 270360 B/op;    3 allocs/op
Durable hydrate:      52207  52370  55446  52913  63681 ns/op; 401537 B/op;    6 allocs/op
```
