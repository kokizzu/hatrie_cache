# T235 Cooperative Fiber Worker Pool

`hat/hatFiber.NewWorkerPool` provides an opt-in bridge between the existing
single-owner stackless scheduler and callers that need concurrent submission.
Each worker owns one `hatFiber.Scheduler`; callers submit bounded continuation
callbacks from any goroutine and receive a `Future`.

## Why

The existing `Scheduler` is intentionally the lowest-overhead option, but its
owner must call `Spawn`, `Run`, and `Reap` on one goroutine. The worker pool is
for application paths that need:

- concurrent `Submit` calls;
- bounded admission and backpressure per worker;
- asynchronous completion through `Future.Wait` or `Future.Done`;
- cooperative fairness without one goroutine stack per task.

It does not replace `Scheduler`. Use `New` and `Run` when the caller already
owns the event loop and wants the lowest allocation cost.

## Example

```go
pool, err := hatFiber.NewWorkerPool(hatFiber.WorkerPoolOptions{
	Workers:            4,
	MaxFibersPerWorker: 64,
	QueueCapacity:      256,
	StepsPerTurn:       32,
})
if err != nil {
	return err
}
defer pool.Close(context.Background())

remaining := 3
future, err := pool.Submit(ctx, func(context.Context) (hatFiber.Step, error) {
	remaining--
	if remaining == 0 {
		return hatFiber.StepDone, nil
	}
	return hatFiber.StepYield, nil
})
if err != nil {
	return err
}
return future.Wait(ctx)
```

## Semantics

- The pool is not started by any existing package; construction is explicit.
- `Workers`, `MaxFibersPerWorker`, `QueueCapacity`, and `StepsPerTurn` use
  documented defaults when zero.
- `QueueCapacity` is per worker. Submissions are assigned round-robin and wait
  for that worker's queue or the submission context to be canceled.
- A task must return promptly. `StepYield` is the supported continuation point;
  the pool cannot preempt a callback.
- `StepWait` is for a caller-owned `Scheduler`; pooled callbacks do not expose
  the worker's synchronization primitives.
- A submission context is checked before admission and passed to every step.
  `Future.Wait` has its own wait context and does not cancel the task by itself.
- `Close` rejects new submissions, drains admitted work, and waits for workers.
  A canceled close context cancels remaining work and returns its error.
- Panics in a task are converted to a future error so one task cannot crash the
  worker process.

## Defaults And Limits

| Option | Default | Limit |
| --- | ---: | ---: |
| `Workers` | 1 | 1,024 |
| `MaxFibersPerWorker` | 256 | 1,048,576 total fibers |
| `QueueCapacity` | 256 per worker | 1,048,576 per worker |
| `StepsPerTurn` | 64 | 1,048,576 |

The limits prevent configuration loaded from an untrusted source from
allocating unbounded scheduler or queue storage.

## Benchmark

Workload: 128 tasks, eight cooperative steps per task. The goroutine baseline
uses 128 goroutines and `runtime.Gosched`; the worker pool uses four workers,
32 fiber slots per worker, a queue of 128 per worker, and eight scheduler steps
per turn. The figures below are medians from five clean one-sample benchmark
invocations on the same AMD Ryzen 9 5950X host.

| Workload | Median ns/op | Median B/op | Allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Goroutine baseline | 490,578 | 3,353 | 130 | 1.00x |
| `WorkerPool` | 132,279 | 29,698 | 640 | 3.71x faster |
| Existing single-owner scheduler | 11,824 | 0 | 0 | 41.49x faster than `WorkerPool` |

Raw samples:

```text
Goroutine ns/op: 481024 531097 538017 446619 490578
Goroutine B/op:  3275   3301   3373   3369   3353
WorkerPool ns/op: 136622 132279 136207 131023 131882
WorkerPool B/op:  29698  29699  29698  29699  29699
```

The result is a concurrency/API tradeoff, not a claim that pooled work is
cheaper than the existing scheduler. `WorkerPool` removes goroutine scheduling
cost for this workload but pays for futures, cross-goroutine queues, worker
state, and synchronization. It is therefore opt-in and should be measured
against the caller's actual task mix.

Run the reproducible checks with:

```text
make test-t235
make race-t235
make vet-t235
make benchmark-t235
```
