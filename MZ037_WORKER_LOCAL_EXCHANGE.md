# Worker-Local Exchange Batching

This is the MZ-37 adoption from Materialize-style dataflow exchanges. It adds
an explicit `hatPipeline.WorkerLocalExchange[T]` for workloads where a known
worker can accumulate values before handing them to a partition consumer.

## Why

`PartitionedAsyncBatcher.Submit` sends one request through a channel for every
value. That is simple and preserves the existing behavior, but it makes the
producer pay queue coordination on every record. Worker-local exchange keeps a
private buffer for each `(worker, partition)` pair and transfers a full buffer
as one request.

The feature is opt-in. Existing `AsyncBatcher` and `PartitionedAsyncBatcher`
constructors and defaults do not select it automatically.

## API

```go
exchange, err := hatPipeline.NewWorkerLocalExchange(
	hatPipeline.WorkerLocalExchangeOptions[Row]{
		Workers:       4,
		Partitions:    8,
		Capacity:      1024,
		BatchSize:     64,
		FlushInterval: 10 * time.Millisecond,
		Handler: func(ctx context.Context, partition int, values []Row) error {
			return writePartition(ctx, partition, values)
		},
	},
)
if err != nil {
	return err
}
defer exchange.Close(context.Background())

if err := exchange.Submit(worker, partition, ctx, row); err != nil {
	return err
}
if err := exchange.FlushWorker(worker, ctx); err != nil {
	return err
}
```

`Workers`, `Partitions`, `Capacity`, `BatchSize`, and `FlushInterval` use
sane defaults when zero. Capacity is a queued-value budget distributed across
partitions and must hold at least one complete batch per partition. The queue
uses whole batch slots, so unused remainder values in a partition's budget are
not queued. `Flush` transfers all partial buffers; `FlushWorker` transfers one
worker's buffers. A flush interval bounds already transferred full batches,
while partial local buffers require an explicit flush or close.

The worker index is caller-selected; this is partitioning, not automatic
sharding. One producer must own each worker index, and `Submit`, `Flush`, and
`Close` must not overlap. Values submitted by the same worker to the same
partition retain order. Different workers targeting one partition have no
global ordering guarantee.

The handler must not retain or mutate its values slice after returning. The
exchange reuses completed batch buffers, clearing them before returning them to
its pool. A failed transfer leaves the local buffer intact so the caller can
retry with a live context.

`AsyncBatcher.SubmitBatch` is the lower-level grouped primitive. It transfers
the supplied slice, consumes one queue request, and invokes one handler call;
its `MaxBatchSize` still bounds the grouped request. The existing per-record
`Submit` path remains unchanged.

## Measurement

Measured on Linux `amd64`, AMD Ryzen 9 5950X, five samples, with
`-benchtime=200ms -benchmem`. Both workloads route the same sequential stream
across four partitions. The baseline submits one record at a time through
`PartitionedAsyncBatcher`; the optimized path uses four worker-local buffers,
four partitions, and batch size 64.

| Workload | Median ns/op | B/op | Allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: |
| Per-record dispatch baseline | 94.48 | 0 | 0 | control |
| Worker-local exchange | 12.87 | 0 | 0 | 7.34x faster |

The buffer pool removes the initial grouped-path allocation cost from the
steady-state benchmark. The exchange still retains bounded in-flight batch
buffers: local buffers are bounded by `Workers * Partitions * BatchSize`, and
the downstream queue is bounded by the configured value budget after
conversion to whole batch slots. This bounded retention is the memory tradeoff
for batching, not an unbounded growth path.

Raw paired output from `make benchmark-mz037-after`:

```text
BenchmarkMZ037PerRecordDispatchBaseline-32 2469949 100.4 ns/op 0 B/op 0 allocs/op
BenchmarkMZ037PerRecordDispatchBaseline-32 2568999 86.35 ns/op 0 B/op 0 allocs/op
BenchmarkMZ037PerRecordDispatchBaseline-32 2771029 87.98 ns/op 0 B/op 0 allocs/op
BenchmarkMZ037PerRecordDispatchBaseline-32 2913554 105.4 ns/op 0 B/op 0 allocs/op
BenchmarkMZ037PerRecordDispatchBaseline-32 2597743 94.48 ns/op 0 B/op 0 allocs/op
BenchmarkMZ037WorkerLocalExchange-32 17542320 12.87 ns/op 0 B/op 0 allocs/op
BenchmarkMZ037WorkerLocalExchange-32 17601980 12.66 ns/op 0 B/op 0 allocs/op
BenchmarkMZ037WorkerLocalExchange-32 17855104 12.95 ns/op 0 B/op 0 allocs/op
BenchmarkMZ037WorkerLocalExchange-32 19707151 12.53 ns/op 0 B/op 0 allocs/op
BenchmarkMZ037WorkerLocalExchange-32 17867918 12.87 ns/op 0 B/op 0 allocs/op
```

The repeatable command is `make benchmark-mz037-after`. The baseline-only
control is `make benchmark-mz037-before`.
