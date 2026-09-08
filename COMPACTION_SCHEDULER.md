# Compaction Scheduler

The `hatStorage.CompactionScheduler` is an opt-in maintenance coordinator for persistent-shard compaction. It coalesces repeated requests for the same shard, limits concurrent callbacks, runs queued tasks in deterministic name order, and requeues failures for a later retry.

It has no timer, background goroutine, write-path hook, or storage-engine dependency. Existing behavior is unchanged until a caller creates a scheduler and explicitly calls `Schedule` and `Run`.

## Usage

```go
scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{
    MaxConcurrent: 2,
})
if err != nil {
    return err
}

_, err = scheduler.Schedule("primary", func(context.Context) error {
    _, err := store.Compact(hatCache.LevelDBCompactionOptions{})
    return err
})
if err != nil {
    return err
}

run, err := scheduler.Run(ctx)
if err != nil {
    // Failed tasks remain queued and can be retried by a later Run.
    log.Printf("compaction run failed: %v", err)
}
log.Printf("compaction: scheduled=%d completed=%d failed=%d", run.Scheduled, run.Completed, run.Failed)
```

`MaxConcurrent: 0` selects `DefaultCompactionSchedulerMaxConcurrent` (1). A negative value is rejected. Repeated `Schedule` calls for a queued or running task name return `(false, nil)` and do not create overlapping work.

## Operational Rules

- Schedule one stable name per persistent shard or key range.
- Choose concurrency from the storage device and memory budget; the scheduler does not estimate safe parallelism.
- Use a context with an operator deadline. The callback receives it and should honor cancellation.
- Inspect `CompactionRun.Failed` and retry the remaining queue after the underlying failure is understood.
- Run compaction outside request handlers unless the request is explicitly an operator maintenance action.

The scheduler is a coordination primitive, not an automatic policy engine. It does not decide when a shard is due, change the compaction range, or provide a distributed lease. Those concerns remain with the deployment and the storage owner.

## Measurement

On the repository benchmark host, draining 64 no-op tasks with a concurrency limit of four measured approximately 26.0 us/op, 17.35 KB/op, and 35 allocations/op. This includes scheduler creation, task registration, sorting, worker startup, and result collection. The scheduler is intended for infrequent maintenance, so these costs are not charged to normal cache reads or writes.
