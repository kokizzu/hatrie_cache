# Asynchronous Batch Ingestion

`hatPipeline.AsyncBatcher` is an opt-in ClickHouse-style asynchronous insert
primitive. It accepts values with bounded backpressure and invokes one handler
for each batch when either the maximum batch size or the flush interval is
reached.

The batcher is not enabled by any server or cache default. Constructing one is
the explicit opt-in. It is an in-memory throughput helper, not a replacement
for `CommandJournal`, the replication outbox, or a durable SQL transaction.

```go
batcher, err := hatPipeline.NewAsyncBatcher(hatPipeline.AsyncBatcherOptions[Write]{
	Capacity:      1024,
	MaxBatchSize:  64,
	FlushInterval: 10 * time.Millisecond,
	Handler: func(ctx context.Context, batch []Write) error {
		return store.InsertBatch(ctx, batch)
	},
})
if err != nil {
	return err
}
defer batcher.Close(context.Background())

if err := batcher.Submit(ctx, write); err != nil {
	return err
}
if err := batcher.Flush(ctx); err != nil {
	return err
}
```

## Defaults

Zero-valued options use these bounded defaults:

| Option | Default | Limit |
|---|---:|---:|
| `Capacity` | 1,024 values | 1,048,576 |
| `MaxBatchSize` | 64 values | 4,096 |
| `FlushInterval` | 10 ms | positive duration |

The queue holds at most `Capacity` submitted values plus one active batch of at
most `MaxBatchSize` values. `Submit` waits for queue capacity and returns the
caller context error when its wait is canceled. A canceled `Close` returns the
caller context error while the batcher continues draining already accepted
values; a later `Close` waits for the final result.

Handlers run serially in submission order. They must finish using the supplied
slice before returning and must not retain or mutate it. A handler error is
reported once by the next `Flush` or `Close`; later batches continue to run.
Failed batches are not retried automatically, so retry or dead-letter policy
belongs in the handler or the durable journal layer.

## Tradeoff

Batching reduces handler calls and allows a storage backend to amortize one
commit, index update, or network request over many values. The cost is bounded
queue memory, one worker goroutine, timer wakeups while a batch is active, and
latency up to `FlushInterval` for a partially filled batch. The ordinary cache,
SQL, and replication paths are unchanged unless a caller constructs a batcher.

The package benchmark target is:

```text
make benchmark-t-async-batcher
```

It reports both `MaxBatchSize=64` and `MaxBatchSize=1` so handler-call
amortization is visible instead of presenting asynchronous submission as a
faster replacement for a direct synchronous write.
