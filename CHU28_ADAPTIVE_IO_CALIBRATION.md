# CH-U28 Adaptive Compaction I/O Calibration

`hatStorage.CompactionIOCalibration` is an opt-in feedback loop for the
existing compaction scheduler. It learns a bounded bytes-per-second rate from
successful compaction callbacks and uses that rate for later
`ScheduleWithIO`/`ScheduleWithPriorityAndIO` waits.

The zero-value scheduler is unchanged. There is no background goroutine, OS
disk probe, timer, or automatic activation. A caller explicitly constructs a
calibrator and passes it as `CompactionSchedulerOptions.IOCalibration`.

## Behavior

- A zero `InitialBytesPerSecond` lets the first valid sample establish the
  rate. The default bounds are 1 MiB/s and 1 TiB/s.
- Each sample is clamped to the configured bounds and blended with the current
  rate using a conservative quarter-step EWMA.
- Only successful callbacks update the estimate. Failed or canceled work does
  not teach the scheduler an untrusted throughput value.
- Calibration is bounded state: one mutex, counters, and no retained task or
  payload data.
- `Reset` clears samples and restores the optional initial rate.

## Example

```go
calibration, err := hatStorage.NewCompactionIOCalibration(
    hatStorage.CompactionIOCalibrationOptions{
        InitialBytesPerSecond: 128 << 20,
        MinBytesPerSecond:     16 << 20,
        MaxBytesPerSecond:     512 << 20,
    },
)
if err != nil {
    return err
}
scheduler, err := hatStorage.NewCompactionScheduler(
    hatStorage.CompactionSchedulerOptions{IOCalibration: calibration},
)
```

The caller still supplies estimated bytes to each scheduled task. The
calibrator changes pacing, not the task's storage semantics.

## Measurement

Machine: AMD Ryzen 9 5950X, linux/amd64. Five `-benchmem` samples were run
before and after the change.

| Benchmark | Before | After | Interpretation |
| --- | ---: | ---: | --- |
| Default scheduler run | 612.6-647.4 ns/op, 760 B/op, 7 allocs/op | 597.9-603.5 ns/op, 760 B/op, 7 allocs/op | No default overhead measured |
| Existing static I/O throttle | 936.6-949.7 ns/op, 1,096 B/op, 11 allocs/op | 909.6-932.3 ns/op, 1,096 B/op, 11 allocs/op | Existing opt-in path unchanged within run variance |
| Calibration `Observe` | not applicable | 6.445-6.886 ns/op, 0 B/op, 0 allocs/op | Feedback update cost |
| Fresh adaptive scheduler run | not applicable | 1,038-1,050 ns/op, 1,160 B/op, 12 allocs/op | Explicit calibration overhead |

The adaptive path costs about 1.7x the default scheduler microbenchmark and
adds about 400 B/op and five allocations in this fresh-scheduler workload.
That cost is isolated to callers that opt in and is paid around maintenance
tasks rather than data writes or reads. The gain is a device-specific pacing
rate that adapts after successful compactions instead of requiring a fixed
operator guess. Production decisions should measure actual compaction latency
and foreground tail latency with the target storage device.

