# MZ011 Kafka-Style Partition Offset Frontiers

MZ011 adopts the partition-frontier idea used by streaming systems such as
Materialize for Kafka-like sources. `hatPipeline.PartitionOffsetFrontier`
tracks the exact completed offset and monotone event-time watermark for every
registered partition. It is adapter-agnostic: a Kafka client or another
partitioned source supplies updates, while the frontier answers whether the
whole source is initialized and exposes a conservative common frontier.

## Usage

```go
frontier, err := hatPipeline.NewPartitionOffsetFrontier(
	[]int32{0, 1, 2},
	hatPipeline.PartitionOffsetFrontierOptions{},
)
if err != nil {
	return err
}

// Offset is the next offset to consume. Watermark is the monotone event-time
// bound below which the partition is complete.
for _, update := range updates {
	if err := frontier.Advance(update.Partition, update.Offset, update.Watermark); err != nil {
		return err
	}
}

if frontier.Ready() {
	offset, watermark, _ := frontier.Common()
	// Every partition covers records below offset and event time below watermark.
	_ = offset
	_ = watermark
}
```

`Advance` accepts equal values idempotently and rejects a lower offset or
watermark. A partition must be registered at construction time; unknown or
negative partition IDs are rejected. The caller owns synchronization, matching
the existing `FrontierAntichain` contract. Use external synchronization when
updates can arrive from multiple goroutines.

`Snapshot` returns a detached, partition-sorted vector. Its `Ready` flag is
true only after every registered partition has published at least one update.
The common offset and watermark are component-wise minima, so a slow partition
conservatively limits the complete source frontier.

## Bounds And Memory

The default maximum is 256 partitions, with a hard maximum of 1,048,576. The
implementation stores partition metadata in a flat slice. Sparse partition IDs
use one lookup map; the common contiguous `0..N-1` layout uses direct indexing
and does not retain that map. The hot `Advance` and `Ready` paths allocate
nothing.

This is a frontier primitive, not a Kafka consumer or an exactly-once commit
protocol. The caller must persist offsets with its source transaction or
checkpoint and must decide how to handle a rebalance or partition loss.

## Cost And Measurement

The final benchmark uses 64 contiguous partitions on Linux/amd64 with an AMD
Ryzen 9 5950X and five samples per case. The manual control is a map of
partition state. The steady-state benchmarks update one partition per
iteration; the common benchmark also computes the minimum across all 64
partitions.

| Operation | Median ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | ---: |
| Manual map advance/readiness | 16.13 | 0 | 0 | 1.00x |
| Dense frontier advance/readiness | 3.702 | 0 | 0 | 4.36x faster |
| Manual map common frontier | 530.3 | 0 | 0 | 1.00x |
| Dense frontier common frontier | 30.75 | 0 | 0 | 17.24x faster |
| Manual map construction | 1,700 | 3,240 | 3 | 1.00x |
| Dense frontier construction | 881.7 | 2,408 | 5 | 1.93x faster, 1.35x lower bytes |

Construction uses two more allocations because the frontier owns a sorted
partition slice and entry slice. That setup cost is paid once; the update and
readiness path is zero-allocation. For sparse IDs, the map fallback preserves
correctness and bounded lookup without claiming the contiguous fast-path
numbers.

Raw final samples (`ns/op`, `B/op`, `allocs/op`):

```text
manual-advance: 15.47 0 0
manual-advance: 15.16 0 0
manual-advance: 15.85 0 0
manual-advance: 15.46 0 0
manual-advance: 16.39 0 0
manual-common: 553.6 0 0
manual-common: 572.2 0 0
manual-common: 563.9 0 0
manual-common: 529.9 0 0
manual-common: 563.2 0 0
dense-advance: 4.027 0 0
dense-advance: 3.717 0 0
dense-advance: 3.596 0 0
dense-advance: 3.552 0 0
dense-advance: 3.619 0 0
dense-common: 30.95 0 0
dense-common: 28.31 0 0
dense-common: 29.49 0 0
dense-common: 28.75 0 0
dense-common: 28.49 0 0
manual-build: 1580 3240 3
manual-build: 1576 3240 3
manual-build: 1574 3240 3
manual-build: 1527 3240 3
manual-build: 1573 3240 3
dense-build: 892.9 2408 5
dense-build: 887.5 2408 5
dense-build: 884.6 2408 5
dense-build: 902.2 2408 5
dense-build: 867.1 2408 5
```

The first pre-implementation baseline run measured the manual map at 15.52
ns/op for advance/readiness and 526.2 ns/op for common-frontier calculation;
the paired final control above is used for relative comparisons because it ran
in the same process as the new implementation.

Run the benchmark with:

```text
make benchmark-mz011-partition-frontier
```

## Verification

Focused tests cover sorted and detached snapshots, incomplete and complete
frontiers, common minima, idempotent updates, offset and watermark regressions,
unknown and invalid partitions, bounds, and the contiguous direct-index fast
path.

```text
make test-mz011-partition-frontier
make format-mz011-partition-frontier
make verify-mz011-partition-frontier-docs
```
