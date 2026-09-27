# MZ-004 Compaction Coalescing

This adds an opt-in duplicate-compaction coalescer, inspired by Materialize's
logical compaction maintenance. It prevents identical work from being queued
more than once while the original request is waiting or running.

## API

The existing constructors and `Submit` methods remain unchanged. Opt in with
one of these constructors:

```go
scheduler, err := hatPipeline.NewCoalescingFrontierCompactionScheduler(
	context.Background(), retention, 2, 128,
)
```

For priority ordering plus coalescing, use
`NewPriorityCoalescingFrontierCompactionScheduler` and
`SubmitPriorityCoalesced`.

```go
accepted, err := scheduler.SubmitCoalesced(
	ctx, "events", boundary, compactTask,
)
if err != nil {
	return err
}
if !accepted {
	// The same frontier/boundary already has pending or running work.
}
```

Coalescing is exact: the key is `(frontierID, boundary)`. Different boundaries
are kept separate because the scheduler cannot assume that arbitrary caller
tasks are safe to replace or merge. A canceled or failed submission releases
its key so a later retry can be accepted.

## Defaults And Tradeoffs

- Coalescing is off by default; the legacy FIFO and opt-in priority paths are
  unchanged.
- The duplicate path takes one small mutex-protected map lookup and avoids a
  scheduler queue entry, task execution, and the associated allocations.
- A workload with many distinct outstanding keys pays for one map entry per
  outstanding request. Keep the queue bounded and use `SetPolicy` when a
  caller also needs a bound on frontier work waiting for admission.
- `accepted=false` means the existing request owns execution; callers should
  treat the operation as idempotent for that exact key.

## Benchmark

The benchmark submits 1024 identical requests per batch, with a blocker task
ensuring all duplicate requests arrive while the first request is pending.
The legacy and coalesced cases use the same workload, including the marker
that verifies the batch drained.

Raw `go test` output, three runs each, on AMD Ryzen 9 5950X:

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `BenchmarkMZ004RedundantCompactionBaseline` | 917,513 / 910,642 / 910,404 | 230,402 / 230,390 / 230,383 | 4,112 / 4,112 / 4,112 |
| `BenchmarkMZ004CoalescedCompaction` | 36,599 / 35,732 / 35,796 | 1,368 / 1,368 / 1,368 | 23 / 23 / 23 |

Using the median run, the opt-in coalesced path is approximately **25.44x
faster**, uses **168.4x less allocated memory**, and performs **178.8x fewer
allocations** for this duplicate-heavy workload. This is a workload-specific
result: it measures avoided compaction work, not a claim that arbitrary unique
tasks become faster.

The pre-change legacy benchmark before the coalescer existed measured
773,048 / 816,864 / 774,426 ns/op, 229,379 / 229,388 / 229,380 B/op, and
4,096 allocs/op. The small difference from the comparable table is the
blocker/marker needed to make the post-change A/B window deterministic.

## Verification

```text
make format-mz004-compaction-coalescing
make test-mz004-compaction-coalescing
make test-mz004-compaction-coalescing-package
make race-mz004-compaction-coalescing
make vet-mz004-compaction-coalescing
make benchmark-mz004-compaction-coalescing
```
