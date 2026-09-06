# Differential Watermark

`DifferentialWatermark` is a small explicit frontier holder for differential
inputs. Create it with one `DifferentialLateDataPolicy`, advance its frontier
monotonically, and apply batches through that policy:

```go
watermark, err := hatSql.NewDifferentialWatermark(hatSql.DifferentialLateDataReject)
if err != nil {
	panic(err)
}
if err := watermark.Advance(100); err != nil {
	panic(err)
}
accepted, err := watermark.Apply(updates)
```

`Advance` accepts repeated values, rejects lower values with
`hatSql.ErrDifferentialWatermarkRegression`, and leaves the previous frontier
unchanged after a rejection. `Frontier` returns the current value. A newly
created watermark starts at zero, so no `uint64` timestamp is late initially.

`Apply` does not advance the frontier. It uses the configured accept, drop, or
reject policy from `DIFFERENTIAL_LATE_DATA.md`, and returned rows own cloned
row maps. The type is intended to have one owner for frontier mutation; callers
that share it concurrently must provide their own synchronization.

## Measured Cost

Benchmark command:

```text
make benchmark-sql-differential-watermark
```

For 1,024 rows and a frontier dropping half the rows, the development machine
measured:

| Metric | Result |
| --- | ---: |
| Time | 88-98 us/op |
| Allocated memory | 212,993 B/op |
| Allocations | 1,025 allocs/op |

The watermark state itself adds no per-row allocation; the measured allocations
come from the selected late-data policy's returned slice and cloned row maps.
