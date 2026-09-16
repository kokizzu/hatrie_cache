# CH-U12 Background Index Rebuild Queue

`hatSql.SQLIndexRebuildQueue` is an importable, bounded maintenance primitive
for callers that need to schedule skip-index or other index rebuild callbacks.
It provides priority ordering, FIFO ordering within one priority, cancellation,
progress, bounded pending work, and a bounded status history.

The queue does not mutate SQL schema automatically. A caller still decides
which index needs rebuilding, checks authorization, performs the rebuild, and
chooses whether to retry a failed callback.

## Safe Defaults

The zero value of `SQLIndexRebuildQueueOptions` keeps background work disabled:
`Workers` is zero and `Start` returns `ErrSQLIndexRebuildQueueDisabled`.
`Capacity` defaults to 64 pending callbacks and `HistoryCapacity` defaults to
256 terminal statuses. The queue is therefore opt-in and has no default
goroutines or query-path cost.

`Capacity` limits queued, not currently running, callbacks. Running callbacks
are bounded by `Workers`. Explicit limits are capped at 100,000 pending tasks,
256 workers, and 100,000 retained statuses.

## Example

```go
queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{
	Capacity:        64,
	Workers:         2,
	HistoryCapacity: 128,
})
if err != nil {
	return err
}
if err := queue.Start(ctx); err != nil {
	return err
}
defer queue.Close()

status, err := queue.Enqueue(hatSql.SQLIndexRebuildRequest{
	ID:       "people.country-v1",
	Name:     "people.country-v1",
	Priority: 10,
	Run: func(ctx context.Context, progress hatSql.SQLIndexRebuildProgressFunc) error {
		for completed, total := 0, 100; completed < total; completed++ {
			if err := rebuildChunk(ctx, completed); err != nil {
				return err
			}
			progress(completed+1, total)
		}
		return nil
	},
})
if err != nil {
	return err
}
_ = status
```

`ID`, `Name`, and `Run` are required. Higher priorities run first; equal
priorities retain submission order. `Flush(ctx)` waits until all queued and
running callbacks are terminal. `Status(id)` returns one task, while
`Snapshot()` returns active tasks and the retained terminal history in
submission order.

## Cancellation And Failure

Canceling a queued task removes it immediately and records `canceled`.
Canceling a running task marks `CancelRequested` and cancels the callback
context; the callback must observe `ctx.Done()` for prompt shutdown. A callback
error records `failed`; a panic is recovered and recorded as a failure. Closing
the queue cancels pending/running work and waits for workers to return. A
callback that ignores its context can therefore delay `Close`.

Progress reports with negative values, a completed value greater than total,
regressing completion, or a changed total are ignored. This keeps status
reporting monotone without allowing a bad callback report to change scheduling.

The queue invokes in-process function values only. It opens no listener and
adds no network protocol. If an administrator exposes enqueue/cancel/status
through an endpoint, that endpoint must apply the service's existing
authentication and authorization rules before accepting task names or work.

## Measurement

Reproduce with `make benchmark-before-chu12-c274` and
`make benchmark-chu12-c274`. Both use clean archive overlays, `GOMAXPROCS=1`,
five 200-ms samples, and the same AMD Ryzen 9 5950X Linux/amd64 host.

| Operation | Median | B/op | Allocs/op | Meaning |
|---|---:|---:|---:|---|
| Direct callback baseline | 1.677 ns/op | 0 | 0 | Existing synchronous call control |
| Queue enqueue, 1 worker | 405.0 ns/op | 280 | 3 | Bounded scheduling and status bookkeeping |

The enqueue path is about 241.5x slower than a bare callback because it performs
locking, heap admission, status creation, cancellation wiring, and history
retention. This is an orchestration cost, not a query regression: the queue is
disabled by default and is intended to replace ad-hoc unbounded maintenance
goroutines, not a direct index rebuild call. The benchmark does not claim a
throughput improvement.

Focused correctness and concurrency coverage is in
`hat/hatSql/ch_u12_index_rebuild_queue_test.go`. Run
`make verify-chu12-c274` for normal tests, the race detector, and vet.
