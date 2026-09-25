# MZ-036 Bounded Fixpoint Scheduler

`hat/hatPipeline.RunFixpoint` is an opt-in, importable worklist primitive for
recursive or iterative dataflow. It is deliberately a library building block:
it does not change SQL planning, server startup, persistence, or the default
execution path.

## Contract

- Work is processed in deterministic FIFO order.
- Keys must be comparable. A key is queued once while pending.
- Repeated pending keys replace the value by default. `Merge` can combine the
  existing and incoming values instead.
- The emitter is callback-based, so a step does not build a temporary output
  slice.
- `MaxSteps` bounds callback invocations. `MaxPending` bounds distinct queued
  keys and retained values. Zero selects bounded defaults of `1 << 20`.
- Context cancellation is checked before work and between steps.
- The scheduler is serial and does not make callbacks concurrent.
- A step must finish using the emitter before returning; retaining or calling
  the emitter asynchronously is unsupported.

The bounds are important for recursive graphs that do not converge. A caller
should choose smaller limits when input size or resource budgets are known.
`ErrFixpointStepLimit` and `ErrFixpointPendingLimit` make a bounded stop
explicit rather than silently returning a partial result.

## Example

```go
step := func(ctx context.Context, item hatPipeline.FixpointItem[int, int], emit hatPipeline.FixpointEmitter[int, int]) error {
	if item.Key == 0 {
		return nil
	}
	return emit(hatPipeline.FixpointItem[int, int]{
		Key:   item.Key - 1,
		Value: item.Value + 1,
	})
}

result, err := hatPipeline.RunFixpoint(ctx,
	[]hatPipeline.FixpointItem[int, int]{{Key: 2047}},
	hatPipeline.FixpointOptions[int, int]{
		MaxSteps:  10000,
		MaxPending: 10000,
		Step:      step,
	},
)
```

`result.Steps` reports processed items, `result.Enqueued` reports distinct
worklist insertions, and `result.MaxPending` reports the largest pending set.

## Measurement

The controlled fixture is a 2,048-node reverse chain. The full-scan baseline
repeatedly scans every graph node until no new node is reached. The scheduler
visits only the newly reached frontier. Each command uses five samples with
`-benchtime=200ms`, `-benchmem`, and an isolated temporary `GOCACHE`.

| Path | Median ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Full-scan baseline | 3,506,413 | 2,048 | 1 | 1.00x |
| Fixpoint scheduler | 73,230 | 392 | 6 | 47.89x faster, 0.19x bytes |

The post-change combined run measured a noisier baseline median of 3,637,813
ns/op and the scheduler median of 73,230 ns/op, or 49.68x faster. The baseline
allocation count is lower because it only allocates its reached bitmap; the
scheduler trades four additional allocations for much lower transient bytes
in this sparse-frontier workload.

Raw pre-change baseline samples:

```text
58 3875181 ns/op 2048 B/op 1 allocs/op
61 3509593 ns/op 2048 B/op 1 allocs/op
73 3301338 ns/op 2048 B/op 1 allocs/op
69 3389487 ns/op 2048 B/op 1 allocs/op
67 3506413 ns/op 2048 B/op 1 allocs/op
```

Raw post-change combined samples:

```text
full scan:
68 3652841 ns/op 2048 B/op 1 allocs/op
67 3723254 ns/op 2048 B/op 1 allocs/op
68 3507898 ns/op 2048 B/op 1 allocs/op
64 3662898 ns/op 2048 B/op 1 allocs/op
67 3637813 ns/op 2048 B/op 1 allocs/op

fixpoint:
3134 75570 ns/op 392 B/op 6 allocs/op
3118 73230 ns/op 392 B/op 6 allocs/op
3135 71982 ns/op 392 B/op 6 allocs/op
3170 73047 ns/op 392 B/op 6 allocs/op
3026 73637 ns/op 392 B/op 6 allocs/op
```

These numbers are not a universal claim. Dense frontiers, expensive callback
work, or callers that already maintain an incremental frontier can change the
tradeoff. Any SQL integration must benchmark its actual operator graph and
retain the existing specialized paths when they win.

The earlier generic SQL-wide M064 fixpoint prototype remains rejected for its
own workload and is not revived by this package primitive.
