# C201 Adaptive Asynchronous-Batch Flush

This is a ClickHouse-inspired extension to `hatPipeline.AsyncBatcher`. When
`AdaptiveFlush` is enabled, the worker estimates the accepted-value arrival
rate and adjusts the timer toward the configured target batch size. The
feature is opt-in; the existing fixed-interval behavior remains the default.

```go
batcher, err := hatPipeline.NewAsyncBatcher(hatPipeline.AsyncBatcherOptions[Write]{
	Capacity:                  1024,
	MaxBatchSize:              64,
	FlushInterval:             10 * time.Millisecond,
	AdaptiveFlush:             true,
	AdaptiveTargetBatchSize:   32,
	AdaptiveMinFlushInterval:  2 * time.Millisecond,
	AdaptiveMaxFlushInterval: 40 * time.Millisecond,
	Handler: func(ctx context.Context, batch []Write) error {
		return store.InsertBatch(ctx, batch)
	},
})
if err != nil {
	return err
}
defer batcher.Close(context.Background())
```

## Behavior

- `AdaptiveFlush: false` preserves the original fixed `FlushInterval` path.
- The default adaptive target is half of `MaxBatchSize`, with a minimum of one
  value.
- The default adaptive minimum is one quarter of `FlushInterval`; the default
  maximum is four times `FlushInterval`.
- The interval is clamped to the configured minimum and maximum. The initial
  interval is the configured `FlushInterval`.
- Arrival observations use an EWMA. The worker samples one clock timestamp per
  eight accepted values after the initial observation, so the submitter does
  not pay a clock call and no lock is added to `Submit`.
- A batch still flushes immediately at `MaxBatchSize`; adaptive timing only
  controls partially filled batches.
- `Submit`, `Flush`, `Close`, handler ordering, and handler-error reporting keep
  the existing semantics. Adaptive timing does not make a failed batch retry.

Adaptive options are validated at construction time:

- `AdaptiveTargetBatchSize` must be between one and `MaxBatchSize`.
- `AdaptiveMinFlushInterval` must be positive.
- `AdaptiveMaxFlushInterval` must be positive and at least the minimum.

The constructor returns `ErrAsyncBatcherAdaptiveTargetInvalid`,
`ErrAsyncBatcherAdaptiveMinIntervalInvalid`, or
`ErrAsyncBatcherAdaptiveMaxIntervalInvalid` for those respective errors.

## Choosing Bounds

Use a target near the batch size that the handler can process efficiently. Set
the minimum from the latency budget for sparse traffic and the maximum from the
maximum acceptable wait for a partially filled batch. Bounds are important:
without them, a quiet stream could wait too long and a burst could create
unhelpfully small batches.

The adaptive controller only changes the in-memory flush timer. It does not
change durability, replication acknowledgements, queue capacity, storage
format, or wire protocol.

## Measurement

The focused benchmark was run with five one-second samples on one CPU and
`-benchmem`. The raw command and samples are recorded in
[BENCHMARK.md](BENCHMARK.md#c201-adaptive-asynchronous-batch-flush).

| Workload | Before C201 median | C201 fixed median | C201 adaptive median | Allocations |
|---|---:|---:|---:|---:|
| Submit, `MaxBatchSize=64` | 99.61 ns/op | 105.9 ns/op | 105.3 ns/op | 0 B/op, 0 allocs/op |
| Submit, `MaxBatchSize=1` | 210.9 ns/op | not separately measured | not separately measured | 0 B/op, 0 allocs/op |
| Synthetic adaptive observation | not applicable | not applicable | 18.03 ns/op | 0 B/op, 0 allocs/op |

The fixed and adaptive submission numbers are within measurement noise in the
same harness, so this benchmark does not establish a throughput win or a
regression from enabling the controller. The useful result is behavior under
variable arrival rates without allocations: with a 32-value target, the
controller selects about 4 ms at 1,000 values/s and 40 ms at 100 values/s,
subject to the configured bounds. Callers that need fixed timing should leave
`AdaptiveFlush` disabled.
