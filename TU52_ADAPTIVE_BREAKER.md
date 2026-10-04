# T-U52 Adaptive Peer Circuit Breaker

`hatReplication` now provides an opt-in adaptive policy for callers that
already own per-peer breaker state. It does not change the existing fixed
`CircuitBreakerConfig` path or enable itself in `hatCache`.

## Contract

Create a state value with `NewAdaptiveCircuitBreakerSnapshot`, call
`BeforeAdaptiveAttempt` before a request, persist the returned state on the
snapshot, and call `RecordAdaptiveSuccess` or `RecordAdaptiveFailure` after
the request completes.

```go
config := hatReplication.AdaptiveCircuitBreakerConfig{Enabled: true}
state, err := hatReplication.NewAdaptiveCircuitBreakerSnapshot(config)
if err != nil {
	return err
}

decision, err := hatReplication.BeforeAdaptiveAttempt(state, config, time.Now())
if err != nil {
	return err
}
state.State = decision.State
if !decision.Allowed {
	return errors.New("peer circuit is open")
}
```

Failure classes are bounded enum values: `transport`, `timeout`, `overload`,
`protocol`, and `unknown`. Repeated opens increase cooldown by a class-weighted
step, capped at `MaxCooldown`. Successful half-open recoveries increase the
failure threshold and move cooldown toward `MinCooldown`, capped by the
configured bounds. Raw errors are not retained.

## Defaults

The zero value is disabled. When `Enabled` is true, omitted fields use:

| Setting | Default |
| --- | ---: |
| Minimum failures | 3 |
| Maximum failures | 10 |
| Minimum cooldown | 5s |
| Maximum cooldown | 5m |
| Threshold step | 1 |
| Cooldown step | 5s |
| Recovery successes | 2 |

## Measured Cost

Five runs, Go benchmark time 200 ms, AMD Ryzen 9 5950X:

| Path | Median CPU | Memory | Difference |
| --- | ---: | ---: | ---: |
| Fixed admission | 4.24 ns/op | 0 B/op, 0 allocs/op | baseline |
| Adaptive admission | 26.24 ns/op | 0 B/op, 0 allocs/op | 6.20x CPU |
| Fixed failure record | 57.37 ns/op | 40 B/op, 1 alloc/op | baseline |
| Adaptive failure record | 97.41 ns/op | 40 B/op, 1 alloc/op | 1.70x CPU |

The adaptive path is deliberately opt-in. The extra admission work is
allocation-free; failure recording retains the same time-pointer allocation
profile as the existing fixed policy. The benchmark target is:

```text
make benchmark-tu52-adaptive-breaker
```
