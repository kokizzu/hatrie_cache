# M224 Hydration Progress Metrics

M224 extends the M223 hydration lifecycle with an opt-in progress estimate for
operators and monitoring endpoints that need an ETA without adding clock reads
to the hydration hot path.

## API

`HydrationStateMachine.SetRate(unitsPerSecond)` records a caller-owned,
smoothed positive rate. A rate of zero disables ETA calculation. Negative,
NaN, and infinite rates are rejected.

`HydrationStateMachine.Estimate()` returns a detached `HydrationEstimate` with:

- lifecycle state and generation;
- completed, total, and exact remaining units;
- the configured units-per-second rate; and
- `EstimatedRemaining`, rounded up to nanoseconds and capped at the largest
  representable `time.Duration`.

`Begin` clears the rate, so a retry or new generation cannot inherit stale
timing data. Estimates are reported only while a generation is actively
hydrating; ready, cold, and failed views report zero ETA. `Estimate` does not
sample time and `Advance` remains unchanged, so callers can use their own
sampling interval, smoothing, or external progress source.

## Measurement

Five 200 ms samples on an AMD Ryzen 9 5950X:

| Operation | Before M224 | After M224 | Relative |
| --- | ---: | ---: | ---: |
| Existing M223 `Snapshot` | 19.92 ns/op, 0 B/op, 0 allocs/op | 19.82 ns/op, 0 B/op, 0 allocs/op | within noise, 1.01x faster observed |
| New `Estimate` | n/a | 39.16 ns/op, 0 B/op, 0 allocs/op | new opt-in API |
| New `SetRate` | n/a | 4.20 ns/op, 0 B/op, 0 allocs/op | new opt-in API |

The new metrics cost is paid only when monitoring code requests an estimate;
ordinary snapshots, progress updates, and hydration work do not gain a clock
call or an allocation.

## Verification

Focused tests cover exact remaining units, rate changes, ETA rounding, rate
reset between generations, invalid rates, and nil receivers. Focused race
tests and the full `hatPipeline` package tests cover the combined M223/M224
state machine.
