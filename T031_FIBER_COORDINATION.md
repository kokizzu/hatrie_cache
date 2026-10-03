# T-U31 Fiber Coordination

`hat/hatFiber` now includes coordination primitives that park a continuation
instead of blocking a scheduler worker. The feature is opt-in and does not
change `hatPipeline.Channel` or existing pipeline behavior.

## Primitives

- `Signal` wakes one or all currently parked fibers.
- `Channel[T]` is a positive-capacity typed buffered channel. `Send` and
  `Receive` are nonblocking; `ReceiveStep` and `SendStep` atomically register
  a continuation when the operation would block.
- `Condition` provides predicate-owned `Wait`, `NotifyOne`, and `NotifyAll`.
- `Semaphore` provides `TryAcquire`, `AcquireStep`, and `Release`.
- `WaitGroup` provides `Add`, `Done`, and `WaitStep`.

Every `Step` that parks must return the result of the wait immediately:

```go
var consume hatFiber.Step
consume = func(ctx hatFiber.Context) (hatFiber.Step, error) {
	result, err := channel.ReceiveStep(ctx, consume)
	if err != nil {
		return nil, err
	}
	if result.Blocked {
		return nil, nil
	}
	if !result.OK {
		return nil, nil // closed and drained
	}
	use(result.Value)
	return nil, nil
}
```

`ReceiveStep` and `SendStep` perform the state check and waiter registration
under the primitive lock. That prevents the lost-wakeup race between checking
an empty/full predicate and yielding. A parked fiber consumes a live-fiber
slot but not a worker; `Close` and scheduler cancellation remove parked fibers
so `Wait` cannot hang on an abandoned wait.

Channel close stops new sends and lets buffered values drain. A receive from a
closed, drained channel returns `ok=false, nil`. A zero-capacity channel is
rejected because this implementation deliberately provides buffered,
nonblocking handoff rather than a second blocking path.

## Benchmark

Machine: AMD Ryzen 9 5950X, Linux amd64. Five samples with
`go test -benchmem -count=5`, using one integer send/receive round trip per
iteration. The reference is the existing `hatPipeline.Channel` with the same
capacity and workload.

| Operation | Fiber coordination | Existing pipeline channel | Result |
| --- | ---: | ---: | --- |
| CPU | 15.07 ns/op | 89.58 ns/op | 5.95x faster |
| Heap | 0 B/op | 0 B/op | unchanged |
| Allocations | 0 allocs/op | 0 allocs/op | unchanged |

This is a hot buffered round-trip benchmark; parked-fiber behavior is covered
by the focused, repeated, and race tests. Raw output is preserved in
[`T031_BENCHMARK_RAW.txt`](T031_BENCHMARK_RAW.txt) and
[`T031_BENCHMARK_BASELINE_RAW.txt`](T031_BENCHMARK_BASELINE_RAW.txt).
