# T-U30 Cooperative Fiber Scheduler

`hatFiber` is an opt-in Tarantool-inspired cooperative scheduler for workloads
that have naturally resumable steps. It is importable as
`hatrie_cache/hat/hatFiber` and does not change existing goroutine, pipeline,
or SQL defaults.

## Model

Each fiber is a `StepFunc` continuation. A callback does a bounded unit of work
and returns `StepYield` to move to the back of the fixed ready ring, or
`StepDone` to finish. The scheduler never preempts a callback and never starts
a goroutine. This is intentionally stackless rather than a stackful green
thread implementation.

```go
import (
	"context"

	"hatrie_cache/hat/hatFiber"
)

ctx := context.Background()
scheduler, err := hatFiber.New(hatFiber.Options{MaxFibers: 1024})
if err != nil {
	return err
}

remaining := 3
id, err := scheduler.Spawn(func(context.Context) (hatFiber.Step, error) {
	remaining--
	if remaining == 0 {
		return hatFiber.StepDone, nil
	}
	return hatFiber.StepYield, nil
})
if err != nil {
	return err
}

if _, err := scheduler.Run(ctx, 0); err != nil {
	return err
}
if status, err := scheduler.Status(id); err != nil {
	return err
} else if status != hatFiber.StatusDone {
	return scheduler.Reap(id)
}
return scheduler.Reap(id)
```

## Bounds and lifecycle

- `MaxFibers` defaults to 1,024 and is capped at 1,048,576.
- Slot and ready-ring storage is allocated once by `New`; the run loop has no
  per-step allocation.
- `Run(ctx, 0)` drains until the ready ring is empty. A positive limit runs at
  most that many callbacks, which lets callers enforce a service quantum.
- Context cancellation returns `context.Canceled` or `context.DeadlineExceeded`
  and leaves unexecuted work ready for a later run.
- `Cancel` marks a ready fiber terminal. `Status`, `Failure`, and `Reap` expose
  terminal state; reaping is required before a slot can be reused.
- Fiber IDs include a generation, so a stale ID cannot address a reused slot.
- `Scheduler` is single-owner by design. Callers that need cross-goroutine
  submission should put requests behind their own bounded channel or use an
  existing worker pool.

This design adopts the useful Tarantool idea, cooperative fairness with cheap
resumption, without pretending that Go callbacks are preemptible. A callback
that blocks or performs an unbounded loop blocks every other fiber on that
scheduler.

## Measurement

The benchmark runs 256 fibers, each returning through eight steps, and reports
five 50 ms samples on Linux/amd64:

| Workload | Median CPU | Median memory | Median allocations |
| --- | ---: | ---: | ---: |
| Stackless scheduler | 22,508 ns/op | 0 B/op | 0 allocs/op |
| Goroutine plus `runtime.Gosched` control | 1,400,311 ns/op | 7,269 B/op | 266 allocs/op |

The control is a lifecycle comparison, not a claim that arbitrary blocking
goroutines can be replaced safely. It includes goroutine, channel, and wait
group setup; the scheduler reuses its fixed storage. The measured scheduler
path is about 62.2x faster for this small-step workload and has no measured
per-operation heap allocation. Reproduce with `make benchmark-tu30`.

Run `make test-tu30`, `make race-tu30`, and `make vet-tu30` for focused
correctness checks. The package is opt-in; no existing code is routed through
it automatically.
