# M223 Hydration State Machine

This adopts a Materialize-style lifecycle primitive for maintained views that
must report whether they are cold, actively hydrating, ready, or failed.

## API

`hatPipeline.NewHydrationStateMachine()` starts in `cold` state.

- `Begin(total)` starts a new generation and moves the machine to `hydrating`.
- `Advance(units)` records bounded progress without allocating or waking
  waiters.
- `Complete()` moves the generation to `ready` only when all declared work is
  complete.
- `Fail(error)` moves the generation to `failed` and preserves the cause.
- `Reset()` returns a completed or failed generation to `cold`.
- `Snapshot()` returns detached state, generation, completed, total, remaining,
  and failure fields.
- `Wait(ctx)` blocks until `ready`, returns a wrapped failure, or observes
  context cancellation. It never starts hydration itself.

Beginning a zero-work generation publishes `ready` immediately. A running
generation cannot be replaced silently, and progress cannot exceed its total.
The state machine is safe for concurrent snapshots, waiters, and transitions;
the caller remains responsible for ensuring only one hydration owner advances a
generation.

The primitive is opt-in and importable. Existing typed-table arrangement APIs
retain their established `stale`/`ready`/`failed` status strings and are not
silently changed by this feature; a future adapter can map a view's lifecycle
to this state machine when its hydration ownership is explicit.

## Measurement

Five 200 ms benchmark samples on an AMD Ryzen 9 5950X compared the new
snapshot path with a clean M222-equivalent mutex-backed status copy:

| Operation | Baseline | M223 | Relative |
| --- | ---: | ---: | ---: |
| Status snapshot | 14.44 ns/op, 0 B/op, 0 allocs/op | 19.77 ns/op, 0 B/op, 0 allocs/op | 1.37x slower |
| Progress advance | n/a | 5.49 ns/op, 0 B/op, 0 allocs/op | new lifecycle API |
| Full begin/advance/complete lifecycle | n/a | 98.96 ns/op, 224 B/op, 2 allocs/op | once per generation |

The baseline is a comparable status-copy lower bound, not an old public M223
implementation. The added snapshot cost comes from lifecycle/error fields and
the synchronized state machine; reads and progress remain allocation-free.
The channel notification allocations are limited to lifecycle transitions,
not per-unit progress. A read-lock experiment was rejected: it improved
snapshot latency by about 5% but made progress about 60% slower and lifecycle
publication about 15% slower.

## Verification

- Focused lifecycle tests cover cold/hydrating/ready/failed transitions,
  progress bounds, retries, failure-cause matching, zero work, nil inputs,
  waiting, and cancellation.
- Focused race tests and the complete `hatPipeline` package race suite pass.
