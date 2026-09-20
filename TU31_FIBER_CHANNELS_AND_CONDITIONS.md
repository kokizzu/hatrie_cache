# T-U31 Fiber Channels And Conditions

`hatFiber` now includes opt-in, single-owner coordination primitives for the
stackless scheduler:

- `Channel[T]`: bounded typed channels with buffered and zero-capacity
  rendezvous modes.
- `Condition`: predicate-based signal and broadcast queues.
- `Semaphore`: bounded token acquisition and release.
- `WaitGroup`: bounded completion counters.

These primitives do not start goroutines and do not add mutexes. The goroutine
that calls `Scheduler.Run` owns the scheduler and all primitives attached to
it. A callback must return promptly and explicitly yield or park; an OS-blocking
call inside a callback still blocks the owning goroutine.

## Channel Usage

`TrySend` and `TryReceive` are the non-parking operations. On an open channel,
`ErrChannelWouldBlock` means the callback can return `WaitSend` or
`WaitReceive`, then retry the same operation when the fiber resumes:

```go
package main

import (
	"context"
	"errors"

	"hatrie_cache/hat/hatFiber"
)

func run(scheduler *hatFiber.Scheduler, channel *hatFiber.Channel[int]) error {
	_, err := scheduler.Spawn(func(context.Context) (hatFiber.Step, error) {
		value, ok, err := channel.TryReceive()
		if errors.Is(err, hatFiber.ErrChannelWouldBlock) {
			return channel.WaitReceive()
		}
		if err != nil {
			return 0, err
		}
		if !ok {
			return hatFiber.StepDone, nil
		}
		_ = value
		return hatFiber.StepDone, nil
	})
	if err != nil {
		return err
	}
	_, err = scheduler.Spawn(func(context.Context) (hatFiber.Step, error) {
		if err := channel.TrySend(42); errors.Is(err, hatFiber.ErrChannelWouldBlock) {
			return channel.WaitSend(42)
		} else if err != nil {
			return 0, err
		}
		return hatFiber.StepDone, nil
	})
	if err != nil {
		return err
	}
	_, err = scheduler.Run(context.Background(), 0)
	return err
}
```

`NewChannel(scheduler, 0)` creates a rendezvous channel. Positive capacities
retain at most that many values. `Close` rejects new sends, wakes blocked
senders and receivers, and leaves buffered values readable before the closed
and drained result (`ok == false`). Closing an already closed channel is safe.

`WaitSend(value)` deliberately does not retain `value` while parked. The
callback owns the value and must retry `TrySend(value)` after resumption. This
keeps arbitrary payloads out of bounded waiter queues and avoids hidden memory
retention. `WaitReceive` follows the same retry pattern.

## Conditions, Semaphores, And Wait Groups

All wait operations are park hints, not blocking calls. Check the predicate or
attempt the operation first, return the primitive's `Step`, and check again
when the callback resumes.

```go
condition, _ := hatFiber.NewCondition(scheduler)
semaphore, _ := hatFiber.NewSemaphore(scheduler, 0)
group, _ := hatFiber.NewWaitGroup(scheduler, 1)

// In a callback:
if !ready {
	return condition.Wait()
}
if !semaphore.TryAcquire() {
	return semaphore.WaitAcquire()
}
if group.Count() != 0 {
	return group.Wait()
}
```

`Condition.Signal` wakes one waiter and `Broadcast` wakes all live waiters.
`Semaphore.Release(n)` returns `n` tokens and wakes up to `n` waiters.
`WaitGroup.Done` is equivalent to `Add(-1)`; reaching zero wakes all waiters.
Negative counts, counter overflow, invalid release counts, and invalid channel
capacities are rejected. The minimum `int` delta is checked without arithmetic
overflow.

## Bounds And Tradeoffs

- Wait queues are fixed to the scheduler's `MaxFibers` capacity. There is no
  unbounded waiter allocation.
- Channel buffers are bounded by `MaxChannelCapacity`; buffered elements can
  retain references until received or drained.
- Primitives are not safe for concurrent access from multiple goroutines. Use
  an explicit cross-goroutine handoff at the owner boundary when needed.
- Existing goroutine, channel, and pipeline defaults are unchanged. This is an
  opt-in API for workloads that already fit cooperative callbacks.

The benchmark in [BENCHMARK.md](BENCHMARK.md#t-u31-fiber-channels-and-conditions)
measures the steady-state zero-capacity handoff. The reusable fiber scheduler
uses fixed storage outside the timed loop; the comparison creates two
goroutines and a channel for each handoff. It is therefore a lifecycle and
allocation comparison, not a claim that a fiber replaces a blocking goroutine
for every workload.
