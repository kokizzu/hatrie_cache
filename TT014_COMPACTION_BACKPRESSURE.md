# TT-014 Compaction Backpressure

This is the bounded, low-cost part of Tarantool Vinyl-style compaction
control. `hatStorage.CompactionController` can now reject a new maintenance
job when the sum of its pending and running `EstimatedBytes` would exceed a
configured `MaxPendingBytes` limit.

## Configuration

```go
controller, err := hatStorage.NewCompactionController(
    hatStorage.CompactionControllerOptions{
        MaxPending:      64,
        MaxPendingBytes: 256 << 20,
    },
)
```

`MaxPendingBytes=0` is the default and disables byte admission. Existing
callers therefore retain their current behavior. A rejected request returns
`hatStorage.ErrCompactionControllerBackpressure`; callers can retry after a
running job completes. A zero `EstimatedBytes` request does not consume the
byte budget.

The budget counts both pending and running jobs, is protected by the
controller mutex, and is released only when a job succeeds or is removed due
to scheduler admission failure. Failed jobs remain retry-pending and continue
to reserve their estimate.

This is intentionally not described as adaptive disk or latency throttling.
The existing scheduler already provides fixed `MaxIOBytesPerSecond` pacing;
adaptive feedback from foreground latency and disk pressure remains a future
TT-014 extension.

## Verification

The red/green test covers exact-bound admission, pending plus running work,
rejection without queue mutation, capacity recovery, and the default-off
path. `make test-tt014-compaction-package` and
`make race-tt014-compaction-throttle` also pass.

## Benchmark

Host: AMD Ryzen 9 5950X, Linux/amd64. Five samples, `-benchtime=100ms`,
`-benchmem`. The fixture submits 64 distinct 1 KiB jobs to a fresh controller
per benchmark iteration.

| Controller path | Median ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Byte budget off | 35,290 | 36,120 | 183 | 1.00x |
| Byte budget on | 33,641 | 36,120 | 183 | 0.95x observed |

The observed 5% difference is within normal short benchmark variance; the
important result is identical allocations and bytes. The scheduler hot-path
benchmark remained allocation-identical after moving admission to the
controller. Raw samples and the scheduler before/after run are recorded in
`BENCHMARK.md`.
