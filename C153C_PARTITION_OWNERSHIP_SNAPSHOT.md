# C153c Partition Ownership Snapshots

This adds an opt-in persistence and transfer boundary for
`hatPipeline.QueuePartitionOwnership`, extending the ClickHouse-style
metadata-consensus direction without changing routing or migration behavior.

## API

```go
payload, err := ownership.MarshalSnapshot()
if err != nil {
	return err
}

restored, err := hatPipeline.NewQueuePartitionOwnership(
	hatPipeline.QueuePartitionOwnershipOptions{PartitionCount: 1},
)
if err != nil {
	return err
}
if err := restored.RestoreSnapshot(payload); err != nil {
	return err
}
```

`MarshalSnapshot` captures the immutable routing view, including stable and
migrating assignments, owners, targets, fences, generations, and readiness.
`RestoreSnapshot` fully validates the payload before atomically publishing a
new immutable view. A failed restore cannot partially change routing state.

## Format And Safety

- `QPO1` magic and version `1` identify the format.
- Partition records are ordered and carry their partition number, preventing
  duplicate or reordered metadata from being accepted.
- Length-prefixed owner and target strings use bounded `uint16` lengths.
- The total payload is bounded at 64 MiB and partition count is bounded by the
  existing ownership registry limit.
- CRC-32 detects transfer/storage corruption. It is not an authentication
  mechanism; deployments that need authenticity must wrap the payload in their
  existing authenticated control-plane transport.
- Stable records cannot contain migration fields; migrating records require
  distinct non-empty source and target owners.

The format is deterministic for a given routing view and allocates only when a
caller explicitly snapshots or restores. `Snapshot`, `Owner`, and
`OwnerUnchecked` remain allocation-free.

## Measurement

The fixture contains 256 partitions alternating between two owners. Five
samples ran on an AMD Ryzen 9 5950X with `-benchmem`.

| Operation | JSON reference | Binary snapshot | Improvement |
| --- | ---: | ---: | ---: |
| Encode | 54,094 ns/op, 46,031 B/op, 3 allocs | 4,390 ns/op, 9,472 B/op, 1 alloc | 12.32x faster, 4.86x lower bytes, 3x fewer allocs |
| Restore | 350,285 ns/op, 41,792 B/op, 271 allocs | 9,561 ns/op, 25,392 B/op, 259 allocs | 36.64x faster, 1.65x lower bytes, 12 fewer allocs |

The pre-change standalone JSON encode baseline was five samples with a median
of 46,611 ns/op, 46,188 B/op, and 3 allocs/op. The post-change paired JSON
reference is included above to control for benchmark-run variance.

Raw post-change output is recorded in [BENCHMARK.md](BENCHMARK.md#c153c-partition-ownership-snapshots).
