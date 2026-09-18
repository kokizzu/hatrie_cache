# CH-20: Parallel Replica Read Coordinator

`hatPipeline.RunParallelReplicaReads` is an opt-in coordinator for large
reads that have already been split into non-overlapping query partitions. Each
partition is assigned to exactly one replica; the coordinator bounds in-flight
callbacks, preserves input order in the result, and cancels remaining work on
the first reader error.

## Example

```go
tasks := []hatPipeline.ParallelReplicaReadTask[Row]{
	{
		Partition: "region-eu",
		Replica:   "cache-eu-2",
		Read: func(ctx context.Context) ([]Row, error) {
			return readRegion(ctx, "region-eu")
		},
	},
	{
		Partition: "region-us",
		Replica:   "cache-us-1",
		Read: func(ctx context.Context) ([]Row, error) {
			return readRegion(ctx, "region-us")
		},
	},
}

results, err := hatPipeline.RunParallelReplicaReads(
	ctx,
	tasks,
	hatPipeline.ParallelReplicaReadOptions{MaxConcurrency: 8},
)
```

The caller owns partition assignment, transport, retries, authentication,
deduplication across query fragments, and final row merging. This API does not
guess a replica, retry a failed read, or execute duplicate reads for quorum
semantics. Duplicate partition names are rejected before any callback runs.

## Safety and defaults

- zero concurrency uses a conservative default of four workers;
- negative or over-256 concurrency is rejected;
- at most 4,096 tasks are accepted;
- empty partition/replica names and nil callbacks are rejected;
- callbacks must honor `context.Context` cancellation;
- the result row slice is transferred without copying;
- concurrency `1` uses a sequential fast path without worker goroutines.

Existing replication, SQL, and local-partition defaults are unchanged. The
feature is useful when callback latency is dominated by network or storage
waits. It is deliberately not used for tiny local callbacks: worker setup,
queueing, result bookkeeping, and cancellation coordination cost more than a
direct sequential loop in that case.

See [BENCHMARK.md](BENCHMARK.md#ch-20-parallel-replica-read-coordinator) for
the paired measurements and raw samples.
