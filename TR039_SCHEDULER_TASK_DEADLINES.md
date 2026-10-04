# Scheduler Task Deadlines

`hatPipeline.Scheduler` and `hatPipeline.ResizableScheduler` now expose an
opt-in `SubmitWithOptions` method for cooperative task deadlines and caller
cancellation.

```go
err := scheduler.SubmitWithOptions(ctx, hatPipeline.TaskOptions{
	Timeout:         250 * time.Millisecond,
	PropagateCaller: true,
}, func(taskCtx context.Context) error {
	return doWork(taskCtx)
})
```

## Contract

- The zero-value `TaskOptions{}` delegates to the existing `Submit` path.
- `Timeout` starts when `SubmitWithOptions` is called, so queue wait consumes
  the same task budget as execution.
- `Deadline` is an absolute deadline. When both fields are set, the earlier
  deadline wins.
- `PropagateCaller` is disabled by default. The submission context still
  controls whether admission can proceed, but cancellation after admission is
  forwarded into the task only when this flag is true.
- Tasks are cooperative. A deadline does not preempt a task that ignores its
  context, and callers must check `taskCtx.Done()` or `taskCtx.Err()` in
  long-running work.
- A task that returns nil after its deadline is reported as
  `context.DeadlineExceeded`; a task error is preserved when the task returns
  an error first.
- Negative timeouts are rejected with
  `hatPipeline.ErrSchedulerTaskOptionsInvalid`.

The feature is default-off. Existing `Submit` callers do not construct task
contexts or timers, and the resizable scheduler keeps the same behavior.

## Measured Cost

Linux/amd64 on an AMD Ryzen 9 5950X. Each row is five `-benchmem` samples from
the 256-task fixed-scheduler no-op batch benchmark. The deadline case uses an
absolute deadline one hour in the future, so it measures admission and
context-management overhead rather than timeout expiry.

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing submit, clean baseline | 68,932; 63,149; 63,564; 66,039; 63,664 | 63,664 | 2,951 | 12 |
| Deadline options, before allocation reduction | 220,190; 217,472; 215,271; 228,489; 269,334 | 220,190 | 248,965 | 3,086 |
| Deadline options, after allocation reduction | 169,623; 179,442; 166,736; 182,293; 176,456 | 176,456 | 97,413 | 1,294 |

The optimized deadline path reduces cumulative benchmark allocation by 2.56x
and allocations by 2.39x versus the first implementation. It still costs more
than the plain path because each opted-in task needs deadline state and a
timer. That cost is intentional and isolated to callers requesting deadlines
or caller cancellation; it is not paid by the default scheduler API.

The final repeatable run also measured the unchanged submit control at
55,754 ns/op, 2,950 B/op, and 12 allocations/op. The control is included to
show the default path remains allocation-stable; exact CPU comparison across
separate runs is sensitive to host scheduling.

Run the focused checks with:

```sh
make format-tr039-scheduler-task-deadlines
make test-tr039-scheduler-task-deadlines
make race-tr039-scheduler-task-deadlines
make vet-tr039-scheduler-task-deadlines
make benchmark-tr039-scheduler-task-deadlines
```
