# MZ012 Kafka-Style Consumer-Group Fencing

`hat/hatPipeline.ConsumerGroupFence` adopts the consumer-generation fencing
idea used by Kafka-style sources and Materialize connectors. A coordinator
publishes an assignment generation, and workers must present a lease from that
generation immediately before applying a partition side effect. Any later
rebalance makes the old lease stale.

## Usage

```go
fence, err := hatPipeline.NewConsumerGroupFence(
	"orders",
	hatPipeline.ConsumerGroupFenceOptions{MaxPartitions: 256},
)
if err != nil {
	return err
}

_, err = fence.Rebalance([]hatPipeline.ConsumerGroupPartitionOwner{
	{Partition: 0, Member: "consumer-a"},
	{Partition: 1, Member: "consumer-b"},
})
if err != nil {
	return err
}

lease, err := fence.Lease("consumer-a", 0)
if err != nil {
	return err
}
if err := fence.Validate(lease); err != nil {
	return err
}
// Apply the partition side effect only after the last validation.
```

`Rebalance` sorts and validates one complete assignment, then atomically
publishes the next generation. A rejected assignment leaves the previous
generation active. `Lease` rejects an unassigned partition or a member that
does not own it. `Validate` rejects a wrong group, stale generation, removed
partition, or wrong owner.

The fence is deliberately local and transport-neutral. It does not implement
Kafka membership, heartbeats, persistence, or distributed consensus. The
external coordinator must call `Rebalance` from its authoritative assignment
and carry the returned generation into the consumer's side-effect protocol.

## Memory And Concurrency

Published assignment state is immutable and held behind an atomic pointer.
Rebalances are serialized, while `Lease`, `Validate`, and `Snapshot` can run
concurrently with a rebalance without a caller lock. `Validate` and `Lease`
allocate zero bytes. Contiguous assignments starting at partition zero use a
direct owner slice; sparse IDs use a bounded map and a sorted detached
assignment slice.

The default maximum is 256 assigned partitions and the hard maximum is
1,048,576. `MaxPartitions` limits the number of assignments, not the largest
numeric partition ID. Do not copy a fence after first use.

## Measurement

The measurements below were run on Linux/amd64 with an AMD Ryzen 9 5950X,
five samples per case. The manual control uses a map and the same group,
generation, member, and partition checks. The dense rebalance input is 64
already-sorted contiguous partitions, which is the intended fast path.

| Operation | Median ns/op | B/op | allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Manual map validate | 7.686 | 0 | 0 | 1.00x |
| Fenced validate | 7.997 | 0 | 0 | 1.04x higher CPU, same memory |
| Manual map rebalance | 1,767 | 3,496 | 3 | 1.00x |
| Dense fenced rebalance | 1,066 | 3,008 | 3 | 1.66x faster, 1.16x lower bytes |
| Dense fenced snapshot | 462.7 | 1,792 | 1 | Detached inspection path |
| Sparse fenced rebalance, 4 partitions | 522.5 | 608 | 8 | Map fallback |

The small validation cost buys generation and ownership checks that the manual
map does not provide. The dense setup path is faster and smaller because it
avoids the map and uses the common already-sorted assignment form. Sparse
rebalance is bounded but has more short-lived allocations; it is infrequent
control-plane work, not the per-record validation path.

Run the benchmark with:

```text
make benchmark-mz012-consumer-group-fence
```

Raw final samples (`ns/op`, `B/op`, `allocs/op`):

```text
manual-validate: 7.674 0 0
manual-validate: 7.532 0 0
manual-validate: 7.686 0 0
manual-validate: 7.894 0 0
manual-validate: 7.787 0 0
manual-rebalance: 1770 3496 3
manual-rebalance: 1755 3496 3
manual-rebalance: 1717 3496 3
manual-rebalance: 1767 3496 3
manual-rebalance: 1927 3496 3
fenced-validate: 7.943 0 0
fenced-validate: 7.516 0 0
fenced-validate: 8.118 0 0
fenced-validate: 8.073 0 0
fenced-validate: 7.997 0 0
fenced-rebalance: 969.9 3008 3
fenced-rebalance: 1071 3008 3
fenced-rebalance: 1066 3008 3
fenced-rebalance: 1061 3008 3
fenced-rebalance: 1137 3008 3
fenced-snapshot: 490.1 1792 1
fenced-snapshot: 430.6 1792 1
fenced-snapshot: 462.7 1792 1
fenced-snapshot: 504.7 1792 1
fenced-snapshot: 463.9 1792 1
fenced-sparse-rebalance: 599.3 608 8
fenced-sparse-rebalance: 573.7 608 8
fenced-sparse-rebalance: 522.5 608 8
fenced-sparse-rebalance: 502.1 608 8
fenced-sparse-rebalance: 473.2 608 8
```
