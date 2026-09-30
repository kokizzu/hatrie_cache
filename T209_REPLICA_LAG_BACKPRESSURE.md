# T209 Replica Lag Backpressure

`hatReplication.ReplicaBackpressureController` is an opt-in admission gate for
relay or applier batches. It observes a source LSN and the replica's applied
LSN, pauses admission when lag reaches `MaxLag`, and resumes only after lag
falls to `ResumeLag`. The separate thresholds prevent rapid pause/resume
flapping while a replica is near the limit.

```go
controller, err := hatReplication.NewReplicaBackpressureController(
    hatReplication.ReplicaBackpressureOptions{
        MaxLag:    4096,
        ResumeLag: 2048,
    },
)
if err != nil {
    return err
}

admit, state, err := controller.Admit(sourceLSN, appliedLSN)
if err != nil {
    return err
}
if !admit {
    // Stop taking more relay work until the applier catches up.
    return nil
}
_ = state.LagLSN
```

`MaxLag == 0` uses the sane default of 1,024 LSNs. `ResumeLag == 0` defaults
to half of `MaxLag`; an explicit resume threshold must be lower than the pause
threshold. Source and applied LSNs must be monotone. Regressions return an
error without changing controller state, which prevents an old or replayed
observation from accidentally reopening admission.

The controller retains only counters and thresholds. It does not retain keys,
values, or pending batches, so callers remain responsible for queue limits and
for deciding how to retry rejected work.

## Benchmark

Three one-second samples were run on an AMD Ryzen 9 5950X. The direct arithmetic
row is a lower bound, not a replacement for the stateful controller. The
controller is intended to be called once per relay or applier batch, not once
per individual record.

| Operation | Median latency | Heap | Relative latency |
| --- | ---: | ---: | ---: |
| Direct lag arithmetic control | 0.25 ns/op | 0 B/op, 0 allocs/op | 1.00x |
| `ReplicaBackpressureController.Observe` | 25.89 ns/op | 0 B/op, 0 allocs/op | 103.6x direct control |
| Existing `ApplierThrottle.Reserve` | 33.15 ns/op | 0 B/op, 0 allocs/op | 1.28x slower than controller |

The controller adds state validation and hysteresis while remaining allocation
free. It is faster than the existing per-batch throttle control in this
workload, and it does not change the default relay/applier path until callers
explicitly adopt it.
