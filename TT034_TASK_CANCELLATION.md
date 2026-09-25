# TT-034: Cooperative Task Cancellation

`hatFiber.Scheduler` now exposes one scheduler-owned cancellation token and a
bounded shutdown lifecycle without starting any background goroutine.

```go
scheduler, err := hatFiber.New(hatFiber.Options{MaxFibers: 128})
if err != nil {
	return err
}

// Pass the stable token to Run so callbacks share the scheduler lifecycle.
_, err = scheduler.Run(scheduler.Context(), 0)
if err != nil {
	return err
}

scheduler.Close()
stats, err := scheduler.Drain(context.Background())
if err != nil {
	return err
}
fmt.Println(scheduler.DrainState(), stats.Cancelled)
```

`Close` cancels `Scheduler.Context`, prevents new fibers, marks ready and
waiting fibers canceled, and records `DrainStateRequested`. A callback that
closes its owning scheduler is also canceled after its current bounded step.
`Drain` consumes canceled ready-queue entries but does not reap terminal fiber
IDs, so callers can inspect status and call `Reap` explicitly. A canceled
caller context leaves the scheduler in `DrainStateRequested` and a later drain
can resume. Completion is reported only when no active fibers or queued entries
remain.

The existing per-fiber `Cancel` method, `Run` context argument, scheduling
fairness, and zero-value configuration behavior remain unchanged. The feature
is opt-in through `Context`, `Close`, and `Drain`; the scheduler still starts
no goroutines.

## Measurement

Command:

```text
make benchmark-tt034-task-cancellation
```

The existing 256-fiber, eight-step scheduler benchmark was run five times on
Linux/amd64 (AMD Ryzen 9 5950X). The first implementation checked
`context.Err()` after every callback and was rejected: its median was 23,878
ns/op versus the 22,384 ns/op baseline, about 6.7% slower. The final version
uses a direct terminal slot-state check only for the rare close-from-callback
case.

| Version | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Baseline | 22,384 | 0 | 0 |
| Final | 22,620 | 0 | 0 |

Raw samples:

```text
baseline: 22708, 23158, 22105, 22384, 20717
rejected context-check: 24697, 25932, 23325, 23878, 23522
final: 22785, 22620, 21472, 23309, 21393
```

The final median is about 1.1% above the baseline and within the observed
run-to-run spread, with no allocation increase. The expensive design was
removed before the feature was retained.
