# M226 Durable Consensus Metadata

M226 adds an importable `hatTopology.DurablePartitionOwnershipStore` for the
small, crash-recoverable consensus result that binds one state shard to its
current owners and source frontier.

## Why

`EvaluatePartitionOwnershipConsensus` already produces a deterministic quorum
decision, but that decision was previously transport-neutral and in-memory.
After a restart, a caller could not recover the last committed ownership,
consensus sequence, or certified source frontier from a compact durable record.
M226 persists that metadata without putting disk I/O on the routing hot path.

## API

```go
store, err := hatTopology.OpenDurablePartitionOwnershipStore("state/shard-7.ownership")
if err != nil {
	return err
}

record, ok := store.Snapshot()
if ok {
	// Recover record.Sequence, record.Frontier, and record.Decision.
}

record, err = store.Commit(consensusIndex, sourceFrontier, decision)
```

The file is treated as empty only when it does not exist. A malformed,
truncated, checksum-invalid, or unsupported record fails closed instead of
silently becoming empty state.

## Record guarantees

- `Sequence` must be non-zero and strictly increase.
- `Frontier` cannot move backwards.
- A commit must contain a satisfied `PartitionOwnershipConsensusDecision`.
- The shard ID cannot change after the first commit.
- A lower fencing token is rejected.
- A different ownership snapshot with the same fencing token is rejected.
- `Snapshot` and returned commit records own their slices; callers cannot
  mutate the store through them.

The record uses a bounded binary `HPO1` format with unsigned varints and a
CRC32C checksum. Payloads, strings, and member lists are bounded before
allocation. CRC32C detects corruption; it is not an authentication mechanism.
Use the existing authenticated control-plane or journal path when metadata
must be protected against an active attacker.

## Crash safety and writers

Each commit writes a mode `0600` temporary file, flushes the file, atomically
renames it over the destination, and flushes the containing directory. A
failed write does not update the in-memory snapshot. Temporary files are
created in the destination directory so rename stays on one filesystem.

The store serializes callers sharing one process. It does not coordinate
independent processes. Pair one store with
`hatStorage.PersistentShardLease` (M225) or another cross-process single
writer/fencing authority before allowing multiple processes to commit the
same path. This keeps the persistence primitive small and avoids pretending a
local file is a distributed consensus log.

## Placement in the ownership lifecycle

1. Propose ownership and collect votes through the caller-owned control plane.
2. Call `EvaluatePartitionOwnershipConsensus`.
3. After the caller has durably committed the consensus sequence, call
   `Commit` with that sequence, source frontier, and satisfied decision.
4. On restart, open the store before serving the shard and reject work whose
   sequence, frontier, or fencing token is older than the recovered record.

Commit this metadata on ownership/frontier checkpoints, not once per data
row. The durable commit is intentionally much slower than the existing
in-memory quorum calculation.

## Measured tradeoff

On the benchmark host, the existing in-memory quorum calculation stayed near
one microsecond, 1,056 B/op, and 10 allocs/op after M226. The new durable
operations measured approximately:

| Operation | Median | Memory | Allocations | Notes |
| --- | ---: | ---: | ---: | --- |
| Existing in-memory quorum | 992 ns/op | 1,056 B/op | 10 | No disk I/O |
| Durable `Commit` | 2.30 ms/op | 2,688 B/op | 32 | Temp write, file sync, rename, directory sync |
| Durable `Snapshot` | 115 ns/op | 96 B/op | 3 | In-process detached copy |
| Durable reopen + decode | 9.32 us/op | 1,672 B/op | 23 | One small file read and validation |

The commit cost is the explicit tradeoff for crash recovery and should be
amortized over a checkpoint or ownership transition. The existing quorum
benchmark is included as a hot-path regression guard, not as a claim that a
durable commit is as cheap as an in-memory calculation. See the raw output in
`BENCHMARK.md`.
