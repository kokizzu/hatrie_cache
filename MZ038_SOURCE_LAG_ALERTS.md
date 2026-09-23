# MZ038 Source Lag Alerts

`hatPipeline.SourceLagAlertRegistry` provides bounded Materialize-style source
lag alert state. A source enters `warning` at `warning_lag`, enters `critical`
at `critical_lag`, and returns to `healthy` only at or below `recovery_lag`.
This hysteresis prevents alert flapping when lag oscillates around a threshold.

The default policy is:

| Setting | Default |
| --- | ---: |
| Warning lag | 100 |
| Critical lag | 1,000 |
| Recovery lag | 50 |
| Maximum sources | 1,024 |
| Maximum source-name bytes | 256 |

```go
registry, err := hatPipeline.NewSourceLagAlertRegistry(
    hatPipeline.SourceLagAlertRegistryOptions{},
)
transition, err := registry.Observe("orders", lag)
snapshot := registry.SnapshotState()
err = registry.Restore(snapshot)
```

`SnapshotState` is deterministic and `Restore` validates source names, limits,
duplicate entries, policy equality, and impossible state/lag combinations before
atomically replacing the live map. A source limit or invalid snapshot cannot
silently evict or alter an existing alert.

## Measured Cost

On Linux `amd64` with an AMD Ryzen 9 5950X, five samples measured an existing
source observation at a median `44.43 ns/op`, `0 B/op`, and `0 allocs/op`. A
direct threshold-only control measured `1.507 ns/op`; the registry overhead is
the synchronization and bounded map update required for concurrent, durable
state. The registry is therefore intended for source-status/monitoring paths,
not per-row query execution.

Raw output is recorded in `BENCHMARK.md` under `MZ038 Source Lag Alerts`.
