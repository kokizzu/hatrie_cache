# CH-U28 Disk-I/O Merge Throttling

`hatStorage.CompactionScheduler` can pace background compaction starts by an
estimated disk-work cost. This provides a simple shared byte budget for a
scheduler, similar to a background-merge bandwidth limit: foreground callers
can keep the budget conservative, while compaction callbacks retain the
existing retry and concurrency behavior.

## Usage

```go
scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{
	MaxConcurrent:       2,
	MaxIOBytesPerSecond: 256 << 20,
})
if err != nil {
	return err
}

queued, err := scheduler.ScheduleWithIO(
	"orders:part-17",
	512<<20, // estimated read plus write bytes for this merge
	func(ctx context.Context) error {
		return compactOrdersPart(ctx, "part-17")
	},
)
if err != nil || !queued {
	return err
}
_, err = scheduler.Run(ctx)
```

Use `ScheduleWithPriorityAndIO` when the existing priority scheduler is also
needed. A zero `MaxIOBytesPerSecond` is the default and disables the throttle;
legacy `Schedule` and `ScheduleWithPriority` remain unthrottled. A zero task
estimate also bypasses the byte gate.

The rate gate reserves a virtual start time for each nonzero estimate. The
first task can start immediately, and later tasks are paced by their estimated
bytes while still respecting `MaxConcurrent`. Waiting honors the `Run` context;
if it is canceled before the callback starts, the existing scheduler marks the
task failed and requeues it for a later retry. A callback error follows the
same retry path as before.

`CompactionSchedulerStats` reports `IOBytesPerSecond`,
`IOThrottledTaskCount`, `IOThrottledBytes`, and `IOWaitNanoseconds`. The
implementation does not inspect OS disk counters or automatically measure
foreground latency. The caller should calibrate `MaxIOBytesPerSecond` from its
storage and latency budget; this keeps the API portable and avoids hidden I/O
sampling or background goroutines.

## Measurement

Five `-benchmem` samples were measured on Linux/amd64 with an AMD Ryzen 9
5950X. The cold benchmarks create a scheduler per iteration; the warm
benchmarks reuse one scheduler and measure the steady-state scheduling path.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Pre-feature `Schedule`, cold | 679.6; 686.3; 702.8; 714.1; 704.2 | 702.8 | 680 | 7 | 1.00x |
| Legacy `Schedule`, cold after feature | 729.7; 735.6; 735.8; 728.7; 727.8 | 729.7 | 696 | 7 | 1.04x |
| Opt-in `ScheduleWithIO`, cold | 1115; 1148; 1220; 1111; 1185 | 1148 | 1032 | 11 | 1.57x vs legacy cold |
| Legacy `Schedule`, warm | 322.0; 294.3; 303.7; 303.5; 304.4 | 303.7 | 40 | 2 | 1.00x |
| Opt-in `ScheduleWithIO`, warm | 407.2; 414.3; 396.5; 411.2; 429.7 | 411.2 | 40 | 2 | 1.35x vs legacy warm |

The feature adds a small scheduler-object footprint to the default path, but
keeps legacy queue entries unchanged. The meaningful throttled path has no
additional steady-state allocations or bytes in the warm benchmark; its CPU
cost is the intentional rate reservation and lock work.

Run the focused checks with:

```text
make test-chu28
make test-chu28-package
make race-chu28
make vet-chu28
make benchmark-chu28
```
