# Compaction Scheduler Statistics

`hatStorage.CompactionScheduler.Stats()` provides a low-cost, read-only
snapshot of caller-driven maintenance work. It is the first bounded slice of
the ClickHouse background-task and Tarantool `box.stat` ideas: operators can
see queue depth, in-flight work, and callback outcomes without changing the
scheduler's retry, coalescing, or concurrency behavior.

The scheduler remains fully caller-driven. `Stats()` does not start a worker,
timer, exporter, or background goroutine. A zero-value `MaxConcurrent` option
still selects the existing serialized default.

## Usage

```go
package main

import "hatrie_cache/hat/hatStorage"

func inspect(scheduler *hatStorage.CompactionScheduler) {
	stats := scheduler.Stats()
	_ = stats.Pending
	_ = stats.Running
	_ = stats.Completed
}
```

The method is safe to call while `Schedule` or `Run` is active. A nil
scheduler returns the zero value.

## Fields

| Field | Meaning |
|---|---|
| `MaxConcurrent` | Effective callback concurrency limit selected at construction. |
| `Pending` | Unique task names queued and not yet started. |
| `Running` | Unique task names currently executing callbacks. |
| `Scheduled` | Callback attempts selected by `Run`; duplicate requests coalesced by `Schedule` are not counted. Failed callbacks are counted again when retried. |
| `Completed` | Callback attempts that returned `nil`. |
| `Failed` | Callback attempts that returned an error and were requeued for retry. |

`Pending + Running` is a point-in-time queue view. The three cumulative
counters are protected by the scheduler mutex and are monotonic until a new
scheduler is created. They count task executions rather than API calls to
`Schedule`.

This first API intentionally does not report bytes or task age. The scheduler
accepts arbitrary callbacks and cannot infer their storage size or meaningful
work timestamp without inventing unsafe metadata. A future typed task contract
can add those fields without changing this snapshot's semantics.

## Cost

The direct stats call takes the existing scheduler mutex and copies six scalar
counters plus two map lengths. It allocates zero bytes. The benchmark command
is:

```text
make benchmark-tt036-scheduler-stats
```

Five one-second Linux `amd64` samples on an AMD Ryzen 9 5950X measured
`BenchmarkCompactionSchedulerStats` at a median of 13.24 ns/op, 0 B/op, and
0 allocations. The existing 64-task drain stayed at the same 35 allocations
and within measurement noise; raw samples and the fixed scheduler-state
overhead are in [BENCHMARK.md](BENCHMARK.md#tt-036-ch-027-compaction-scheduler-statistics).
