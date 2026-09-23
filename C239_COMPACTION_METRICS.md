# C239 Compaction Metrics

This feature adds low-cost observability for caller-driven compaction:

- `CompactionSchedulerStats.PendingEstimatedBytes` reports the estimated bytes still queued.
- `CompactionSchedulerStats.RunningEstimatedBytes` reports the estimated bytes currently executing.
- `CompactionArrangementDiagnostics.SuccessfulInputBytes` and `SuccessfulOutputBytes` accumulate successful compaction work.
- `CompactionArrangementDiagnostics.WriteAmplification` is successful output bytes divided by successful input bytes.

The estimates are caller supplied through `ScheduleWithIO` and `ScheduleWithPriorityAndIO`. A zero estimate is excluded. Priority-queue entries are counted as pending until execution starts, and failed work is returned to the pending byte total for retry. Successful-only diagnostic totals intentionally exclude failed attempts so a failed write cannot distort the amplification signal.

The scheduler still has no background worker and no automatic compaction. Ordinary scheduling without an estimate keeps its previous lazy I/O-state behavior. A nonzero estimate creates the small per-scheduler estimate map that was already needed by the I/O pacing feature.

## Benchmark

Command: `make benchmark-c239` on Linux/amd64, AMD Ryzen 9 5950X. Five runs were collected for each benchmark; the table reports the median. Baseline values were collected before C239's metric fields and counter maintenance were added.

| Operation | Baseline | C239 | Change | Allocations |
| --- | ---: | ---: | ---: | ---: |
| Diagnostics record | 38.59 ns/op | 38.82 ns/op | 1.006x, +0.6% | 0 B/op, 0 allocs/op |
| Scheduler stats read | 7.651 ns/op | 8.190 ns/op | 1.070x, +7.0% | 0 B/op, 0 allocs/op |

The first implementation summed the estimate map on every stats read and measured about 58 ns/op, roughly 7.6x the baseline. It was rejected. The accepted implementation maintains pending/running byte totals during queue transitions, reducing the read to about 8 ns/op while retaining exact normal-range accounting.

## Verification

Focused tests cover queued, running, completed, priority, retry, zero-estimate, successful-amplification, and failed-observation behavior. The final verification also runs the package tests, race detector, `go vet`, and documentation checks through the C239 Makefile targets.
