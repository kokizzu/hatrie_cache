# T-U30 Cooperative Fiber Scheduler

`hat/hatFiber` provides bounded, stackless cooperative fibers. A fiber is a
continuation: one `Step` performs a short unit of work and returns the next
`Step` to yield. A fixed worker set resumes ready continuations in FIFO order.

This is intentionally opt-in. Existing `hatPipeline.Scheduler` and
`hatPipeline.WorkStealingPool` keep their current task APIs and behavior.

## API

```go
package main

import (
	"context"
	"log"

	"hatrie_cache/hat/hatFiber"
)

func main() {
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       4,
		MaxFibers:     1024,
		QueueCapacity: 1024,
	})
	if err != nil {
		log.Fatal(err)
	}

	remaining := 3
	var step hatFiber.Step
	step = func(ctx hatFiber.Context) (hatFiber.Step, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining--
		if remaining == 0 {
			return nil, nil
		}
		return step, nil
	}
	if _, err := scheduler.Spawn(context.Background(), step); err != nil {
		log.Fatal(err)
	}
	if err := scheduler.WaitIdle(context.Background()); err != nil {
		log.Fatal(err)
	}
	scheduler.Close()
	if err := scheduler.Wait(); err != nil {
		log.Fatal(err)
	}
}
```

`Spawn` is non-blocking with respect to capacity. It returns
`hatFiber.ErrSchedulerFull` when `MaxFibers` or `QueueCapacity` is exhausted,
so callers can choose to retry, shed work, or apply external backpressure.
`WaitIdle` waits for admitted fibers while keeping workers and slots reusable.
`Close` rejects new fibers and drains admitted fibers; `Cancel` stops queued
fibers and asks running steps to observe `Context.Err()` and return.

Zero-valued options use `Workers=1`, `MaxFibers=1024`, and
`QueueCapacity=1024`. `QueueCapacity` must be at least `MaxFibers` so a
continuation can always yield without blocking every worker.

## Semantics

- A step is never preempted. It must do bounded work and return voluntarily.
- Returning another step yields and resumes that continuation later.
- Returning `nil, nil` completes the fiber.
- The first non-cancellation step error stops the scheduler and is returned by
  `Wait` or `WaitIdle`.
- `Context.Err()` observes both the context passed to `Spawn` and scheduler
  cancellation without allocating a derived context per fiber.
- The scheduler is stackless. Code that needs to suspend an ordinary call
  stack must be written as an explicit continuation/state machine.

## Benchmark

Machine: AMD Ryzen 9 5950X, Linux amd64. Five samples, `go test -benchmem
-count=5`, 256 logical flows, eight steps per flow. The reference performs
the same stateful workflow with one goroutine per flow. The recommended
steady-state path constructs the scheduler once and reuses it with `WaitIdle`.

| Path | Median ns/op | B/op | allocs/op | Reference speed | Tradeoff |
| --- | ---: | ---: | ---: | ---: | --- |
| Continuation fibers, steady state | 183,234 | 10,384 | 771 | 5.99x faster | 1.27x B/op, 1.50x allocs |
| Continuation fibers, create/drain per batch | 166,549 | 26,501 | 785 | 6.60x faster | 3.23x B/op, 1.53x allocs |
| Goroutine per flow reference | 1,098,759 | 8,209 | 513 | 1.00x | baseline |

The feature is a CPU-throughput and bounded-worker optimization, not a claim
that every allocation metric is lower. Long-lived callers should reuse one
scheduler; repeatedly constructing schedulers pays the fixed queue and slot
pool cost. Raw output is in
[`T030_BENCHMARK_RAW.txt`](T030_BENCHMARK_RAW.txt) and
[`T030_BENCHMARK_BASELINE_RAW.txt`](T030_BENCHMARK_BASELINE_RAW.txt).
