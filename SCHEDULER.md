# Cooperative Task Scheduler

`hat/hatPipeline.Scheduler` is an opt-in fixed-worker scheduler for bounded
cooperative tasks. It uses the typed channel queue, so submission applies
backpressure instead of growing an unbounded task list.

```go
scheduler, err := hatPipeline.NewScheduler(ctx, 4, 128)
if err != nil {
	return err
}
defer scheduler.Close()

if err := scheduler.Submit(ctx, func(taskContext context.Context) error {
	return processBatch(taskContext, batch)
}); err != nil {
	return err
}
if err := scheduler.Wait(); err != nil {
	return err
}
```

`Close` rejects new submissions and drains tasks already accepted. `Wait`
returns the first task error and cancels other workers. `Cancel` stops queued
work and causes `Wait` to return `context.Canceled` when no task has failed.
Tasks are not preempted; long-running tasks must observe their context. A
queue capacity of zero provides direct handoff, while positive capacity
provides bounded buffering. Existing `Pipeline` behavior is unchanged.
