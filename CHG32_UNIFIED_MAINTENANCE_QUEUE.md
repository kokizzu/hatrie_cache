# CH-G32: Unified Async Maintenance Queue

`hatSql.SQLMaintenanceQueue` is an importable, opt-in queue for table
maintenance callbacks such as `OPTIMIZE`, part merge, index rebuild, and
mutation work.

## Why

ClickHouse keeps heavy maintenance out of the foreground command path and
exposes bounded progress and lifecycle state. Hatrie already had separate
job, refresh, and index-rebuild helpers. This queue gives callers one bounded
priority/cancellation/history surface without forcing a storage engine or
SQL parser integration.

## Example

```go
queue, err := hatSql.NewSQLMaintenanceQueue(hatSql.SQLMaintenanceQueueOptions{
	Capacity:        64,
	Workers:         1,
	HistoryCapacity: 256,
})
if err != nil {
	return err
}
defer queue.Close()

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
if err := queue.Start(ctx); err != nil {
	return err
}
_, err = queue.Enqueue(hatSql.SQLMaintenanceJobRequest{
	ID:       "orders-optimize-20261006",
	Name:     "optimize orders",
	Kind:     hatSql.SQLMaintenanceJobOptimize,
	Priority: 10,
	Run: func(ctx context.Context, progress hatSql.SQLMaintenanceProgressFunc) error {
		// Perform an atomic caller-owned maintenance operation.
		progress(1, 1)
		return nil
	},
	Verify: func(context.Context) error {
		// Optionally verify the newly published state.
		return nil
	},
})
if err != nil {
	return err
}
return queue.Flush(ctx)
```

Supported kinds are `optimize`, `merge`, `index_rebuild`, `mutation`, and
`custom`. Priorities are descending; equal priorities are FIFO. `Cancel`
propagates context cancellation to running callbacks and immediately removes
queued callbacks. `Status` and `Snapshot` expose queued, running, succeeded,
failed, and canceled states with monotone progress and bounded terminal
history.

## Defaults and scope

`Workers: 0` is the default and keeps the queue disabled. Enqueueing is
allowed for inspection, while `Start` and `Flush` return the disabled error.
No background goroutine, SQL `OPTIMIZE` command, filesystem operation, or
automatic schema mutation is introduced. The caller owns the callback,
atomic publication, persistence, and maintenance-window policy.

Callbacks must honor their context and avoid publishing partial state on
failure. `Verify` runs before a task becomes successful. Capacity and history
limits are bounded by the same conservative limits used by the existing index
rebuild queue.

## Measured tradeoff

Paired `BenchmarkCHG32*EnqueueStatus` on an AMD Ryzen 9 5950X, Go benchmark
`-benchtime=100ms -count=3`:

| Path | Median ns/op | B/op | allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Existing index-only queue | 1,119 | 1,528 | 12 | 1.00x |
| Generic maintenance queue | 1,273 | 1,560 | 14 | 1.14x |

The generic path pays a small wrapper cost for the job-kind metadata and
status conversion. The existing index-only queue keeps its original footprint;
the generic queue is opt-in and does not change ordinary query or maintenance
execution.

## Verification

- `make test-ch-g32`
- `make race-ch-g32`
- `make vet-ch-g32`
- `make benchmark-ch-g32`

The full package command was also run with a temporary compatibility shim for
three missing symbols already absent from this base checkout. It reached the
tests and retained the unrelated pre-existing M-U05 arrangement checkpoint
failures; the shim is not part of this feature.
