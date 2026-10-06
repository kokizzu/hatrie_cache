# M-U10 Automatic Temporal-Join Compaction

This adopts the Materialize/timely idea of logical compaction driven by sealed
frontiers, while keeping the scheduler explicitly opt-in. The existing
`DifferentialTemporalJoinCompactionPolicy`, `CompactionRecommendation`, and
`CompactIfNeeded` APIs remain available for callers that want to own the loop.

## API

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

scheduler, err := join.StartCompactionScheduler(
	 hatSql.DifferentialTemporalJoinCompactionSchedulerOptions{
		Context:  ctx,
		Interval: 5 * time.Second,
		Frontiers: func(ctx context.Context) (uint64, uint64, error) {
			return source.SealedFrontiers(ctx)
		},
		OnResult: func(result hatSql.DifferentialTemporalJoinCompactionResult) {
			if result.Err != nil {
				logger.Error(result.Err)
			}
		},
		},
	)
if err != nil {
		return err
}
defer scheduler.Stop()
```

The frontier callback receives the scheduler context and must return the
latest sealed left and right frontiers. A provider error is reported through
`OnResult` and retried on the next tick. `OnResult` is synchronous on the
scheduler goroutine, so it should return promptly and must not call `Stop`.

## Defaults And Safety

- No scheduler is created by `NewDifferentialTemporalJoin` or `ApplyLeft`/
  `ApplyRight`; callers must explicitly call `StartCompactionScheduler`.
- A nil compaction policy rejects scheduler startup with
  `ErrDifferentialTemporalJoinCompactionSchedulerDisabled`.
- A zero interval selects the conservative one-second tick. Negative intervals,
  missing frontier providers, and duplicate schedulers are rejected.
- Only one scheduler may run for a join. `Stop` is idempotent, waits for the
  goroutine to exit, and context cancellation also releases the scheduler slot.
- Compaction still enforces monotone frontiers and collects candidate keys
  before deleting anything. Cancellation therefore leaves the join unchanged.
- A scheduler does not invent or advance frontiers. The caller remains
  responsible for source correctness and for choosing an interval appropriate
  to its workload.

## Tradeoff Measurement

Environment: Linux/amd64, AMD Ryzen 9 5950X 16-Core Processor. Results below
are raw `go test -benchmem` samples from the package benchmarks. The scheduler
benchmarks call one tick directly to isolate scheduler work from wall-clock
ticker noise.

| Path | Samples | Median time | Median heap | Median allocs | Relative |
| --- | --- | ---: | ---: | ---: | ---: |
| Existing adaptive recommendation | 75.45, 74.48, 77.63, 76.26, 75.88 ns/op | 75.88 ns/op | 0 B/op | 0 | baseline |
| Scheduler idle tick | 79.57, 79.26, 78.39, 78.19, 78.58 ns/op | 78.58 ns/op | 0 B/op | 0 | 1.04x, about 3.6% slower |
| Existing manual compaction | 262841, 244354, 251255 ns/op | 251255 ns/op | 197082 B/op | 20 | baseline |
| Scheduler compaction tick | 250027, 247946, 252782 ns/op | 250027 ns/op | 197078 B/op | 20 | 1.00x, within noise |

The scheduler adds one goroutine and ticker only after explicit startup. Its
idle decision path adds no heap allocation and a small measured CPU cost. The
actual compaction path has the same allocation profile and no measured
regression against direct caller-triggered compaction. This is an operational
automation feature, not a claim of faster compaction; deployments that do not
need background work should continue using the default-off caller-driven API.

Commands used:

```text
make codex-mu10-test
make codex-mu10-race
make codex-mu10-package
make codex-mu10-vet
make codex-mu10-benchmark
make codex-mu10-compact-benchmark
```
