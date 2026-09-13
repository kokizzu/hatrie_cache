# C202 Partition-Affine Asynchronous Batching

`hatPipeline.PartitionedAsyncBatcher` is a ClickHouse-inspired way to keep
independent asynchronous-insert buffers aligned with caller-selected
partitions. It is explicit and opt-in: the caller supplies the partition index
for every value. It does not add hash routing, automatic sharding, ownership
movement, persistence, or replication.

```go
batcher, err := hatPipeline.NewPartitionedAsyncBatcher(
	hatPipeline.PartitionedAsyncBatcherOptions[Write]{
		Partitions:    4,
		Capacity:      1024,
		MaxBatchSize:  64,
		FlushInterval: 10 * time.Millisecond,
		Handler: func(ctx context.Context, partition int, batch []Write) error {
			return stores[partition].InsertBatch(ctx, batch)
		},
	},
)
if err != nil {
	return err
}
defer batcher.Close(context.Background())

if err := batcher.Submit(partition, ctx, write); err != nil {
	return err
}
```

## Contract

- `Partitions` must be between 2 and 256. A one-partition configuration is
  rejected because it provides no affinity benefit.
- `Capacity` is the total queued-value budget. It is divided as evenly as
  possible, so the sum of all queue capacities is exactly the configured value;
  capacity must provide at least one slot per partition.
- Each partition has one bounded queue and one worker. Handlers for different
  partitions may run concurrently; values within one partition are handled in
  submission order.
- `Flush` and `Close` wait for every partition concurrently and join handler
  errors in ascending partition order. They do not establish a global handler
  order beyond completion of all partitions.
- `Stats` returns aggregate counters plus an independent snapshot for each
  partition. `Pending` is the sum of all partition pending values.
- `Submit` validates the partition index before touching a queue. A hot
  partition applies backpressure to its own queue rather than consuming the
  capacity of another partition.
- The existing `AsyncBatcher` API and all defaults remain unchanged.

The handler must finish using the supplied batch before returning and must not
retain or mutate it. The partition handler must also be safe to run alongside
handlers for other partitions.

## When To Use It

Use this type when the downstream store has independent partition or shard
writers and can benefit from concurrent batch commits. Keep the ordinary
`AsyncBatcher` when the backend has one serial writer, when cross-partition
ordering is required, or when the workload is too small to amortize multiple
workers. The type is a low-level partition-affinity primitive, not an automatic
cluster sharding system.

## Measurement

The matched benchmark uses four balanced partitions, a 32-round integer mixing
operation per value, and five one-second samples. It runs with the machine's
32 logical CPUs unless noted otherwise.

| Workload | Global worker | Four partition workers | Improvement |
|---|---:|---:|---:|
| CPU work per value | 215.8 ns/op | 69.85 ns/op | 3.09x faster |
| No handler work | 182.0 ns/op | 78.85 ns/op | 2.31x faster |
| CPU work, `-cpu=1` | 186.3 ns/op | 174.4 ns/op | 1.07x faster |
| No handler work, `-cpu=1` | 110.0 ns/op | 104.0 ns/op | 1.06x faster |

All timed submit benchmarks report `0 B/op` and `0 allocs/op`. Construction and
close, measured separately at `-cpu=1`, are the intentional cost:

| Setup and close | Time | Heap | Allocations |
|---|---:|---:|---:|
| One global worker | 2,626 ns/op | 28,528 B/op | 10 |
| Four partition workers | 9,045 ns/op | 31,912 B/op | 58 |

The before-implementation global benchmark was 216.4 ns/op with zero timed
allocations. The post-implementation global control was 215.8 ns/op, so the
new type did not measurably change the existing global path in this run. The
additional setup memory and worker count are why the feature remains explicit.

Reproduce the measurements with:

```text
make benchmark-c202
make benchmark-c202-serial
make benchmark-c202-setup
```
