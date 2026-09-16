# Resizable Scheduler

`hatPipeline.ResizableScheduler` is an opt-in Materialize-inspired execution
primitive for workloads whose useful parallelism changes over time. It keeps a
bounded task queue while allowing the caller to change the worker target
without dropping queued work or preempting a running task.

Materialize uses isolated compute resources for maintained objects and queries.
This package provides the local process equivalent: the caller decides when a
maintenance phase needs more or fewer workers, while the scheduler preserves
task lifecycle correctness. It does not attempt to infer load or resize
automatically.

## API

```go
scheduler, err := hatPipeline.NewResizableScheduler(ctx, 1, 128)
if err != nil {
    return err
}
defer scheduler.Wait()

// A source snapshot is available; increase local parallelism.
if err := scheduler.Resize(4); err != nil {
    return err
}
if err := scheduler.Submit(ctx, refreshTask); err != nil {
    return err
}

// A quiet phase can release idle workers after current tasks finish.
return scheduler.Resize(1)
```

`queueCapacity` may be zero for direct handoff. `WorkerCount` reports the
target and `ActiveWorkerCount` reports workers that have not retired. `Stats`
also reports queue occupancy and close state.

## Correctness

- Increasing the target starts workers immediately.
- Decreasing the target never interrupts a callback. Excess workers retire
  before claiming another task, or after their current task returns.
- Queued tasks stay in the queue during a resize and are drained by `Close`
  and `Wait`.
- `Cancel` cancels the shared task context and may discard queued work, which
  matches the existing fixed `Scheduler` contract.
- A task error cancels the scheduler and is returned by `Wait`.

The scheduler has no implicit persistence, retry, or frontier protocol. Use
`MutationDependencyGraph` when dependencies and resumable maintenance metadata
are needed, and use this type only for execution capacity.

## Defaults And Tradeoff

The existing `hatPipeline.Scheduler` remains fixed-worker and unchanged. The
resizable type is opt-in, so existing callers pay no new hot-path cost.
Resizing uses a broadcast wake channel and atomic worker counters; resize is
expected to be infrequent relative to task execution.

## Benchmark

`make benchmark-mz020-c305` runs five samples of 256 no-op tasks with four
workers, plus repeated one-to-four/to-one resize pairs:

| Case | Raw samples | Median | B/op | allocs/op | Relative |
| --- | --- | ---: | ---: | ---: | ---: |
| Existing fixed scheduler | 66265, 66694, 67534, 65822, 68334 ns/op | 66694 ns/op | 2951 | 12 | 1.00x |
| Resizable scheduler | 72838, 70913, 64916, 67594, 64113 ns/op | 67594 ns/op | 2959 | 12 | 1.013x |
| Resize four-to-one pair | 311.8, 332.4, 332.3, 285.0, 319.5 ns/op | 319.5 ns/op | 242 | 4 | control plane |

The resizable steady-state path is `1.3%` slower in this small local batch,
with the same allocation count. The capability is retained because it adds
online capacity control without changing the default scheduler or task
correctness; callers should use the fixed scheduler when their worker count is
stable. An earlier lock-based worker loop measured `73161 ns/op`; atomic counts
and cached wake channels reduced that median to `67594 ns/op` (`7.6%` faster).
