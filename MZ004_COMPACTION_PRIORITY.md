# MZ-004 Compaction Priority Scheduling

This is an opt-in priority-ordering extension for frontier-gated compaction.
It is inspired by Materialize's need to keep maintenance work responsive while
multiple logical compaction requests are outstanding.

## Behavior

`NewFrontierCompactionScheduler` remains FIFO and is unchanged. Applications
that want priority ordering use:

```go
scheduler, err := hatPipeline.NewPriorityFrontierCompactionScheduler(
    ctx, retention, 2, 256,
)
if err != nil {
    return err
}
defer scheduler.Wait()

err = scheduler.SubmitPriority(ctx, "events", boundary, 100, compactUrgent)
```

Higher integer priorities run first. Equal priorities retain submission order.
Tasks are cooperative and are never preempted after they start. Frontier
retention checks and `MaxOutstanding` policy admission still happen before a
task enters the priority queue. Calling `SubmitPriority` on a legacy FIFO
scheduler returns `ErrSchedulerPriorityUnavailable`.

The queue remains bounded. A zero queue capacity retains direct-handoff
semantics; a positive capacity blocks submitters until capacity or cancellation
is available. `Close`, `Cancel`, task errors, and caller cancellation all wake
blocked submitters and workers.

## Benchmark

Command:

```text
make benchmark-mz004-compaction-priority
```

Machine: AMD Ryzen 9 5950X, Linux amd64. Each benchmark used
`-benchtime=100ms -count=5`; values below are medians of the five raw runs.

| Path | Median ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| FIFO generic submit, bounded queue | 204.7 | 0 | 0 | control |
| Priority generic submit, bounded queue | 49.9 | 0 | 0 | 4.10x faster |
| FIFO frontier submit, bounded queue | 616.6 | 224 | 4 | control |
| Priority frontier submit, bounded queue | 498.7 | 224 | 4 | 1.24x faster |
| FIFO high-priority dispatch behind 128 tasks | 104,802 | 0 | 0 | control |
| Priority high-priority dispatch behind 128 tasks | 63,595 | 0 | 0 | 1.65x faster |

The backlog workload releases one blocked worker with 128 lower-priority
tasks and then one high-priority task. Low-priority tasks perform 1,024 rounds
of deterministic arithmetic. The measured value is the time from releasing
the blocker until the high-priority task runs. Priority scheduling avoids
waiting for the lower-priority backlog.

The pre-change MZ-004 baseline was the existing frontier scheduler with its
direct-handoff queue: `BenchmarkMZ004SchedulerSubmitDefault` had a median of
`874.9 ns/op`, `224 B/op`, and `4 allocs/op`; the policy-enabled path had a
median of `1,737 ns/op`, `424 B/op`, and `8 allocs/op`. Those numbers are kept
as historical context; the bounded controls above are the apples-to-apples
comparison for this feature.

## Tradeoffs

- Priority dequeue is `O(log n)` rather than FIFO `O(1)`, so priority mode is
  intended for queues where avoiding stale low-priority work matters.
- A continuously arriving high-priority stream can starve lower priorities;
  callers should use finite, meaningful priority bands.
- Priority mode is opt-in. Existing FIFO users pay no priority-heap or
  notification overhead and keep their established behavior.
- Automatic SQL planner wiring remains separate MZ-004 follow-up work. Exact
	duplicate coalescing is available through
	`NewCoalescingFrontierCompactionScheduler`; see
	[MZ004_COMPACTION_COALESCING.md](MZ004_COMPACTION_COALESCING.md).

## Verification

```text
make format-mz004-compaction-priority
make test-mz004-compaction-priority
make test-mz004-compaction-priority-package
make race-mz004-compaction-priority
make vet-mz004-compaction-priority
make benchmark-mz004-compaction-priority
```
