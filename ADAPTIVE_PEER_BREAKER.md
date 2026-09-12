# Adaptive Peer Circuit Breaker

`hatPeer.ConnectionPool` already has an optional circuit breaker for one dial
target. The adaptive option adds bounded cooldown backoff for a peer that is
still failing after a half-open probe. This reduces repeated connection work
during a prolonged outage while preserving a shorter cooldown after recovery.

## Defaults

The feature is disabled by default:

- `ConnectionPoolOptions.Breaker == nil`: no circuit breaker.
- `ConnectionPoolCircuitBreakerOptions.Adaptive == nil`: the existing fixed
  threshold and fixed cooldown behavior.
- A non-nil `Adaptive` option enables backoff. Zero fields use sane defaults:
  `MaxOpenInterval` is one minute (or the configured base interval when that
  is longer), and `BackoffFactor` is `2`.
- `BackoffFactor` is bounded to `1..8`; the maximum interval must not be below
  the configured base `OpenInterval`.

## Example

```go
pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
    Breaker: &hatPeer.ConnectionPoolCircuitBreakerOptions{
        FailureThreshold: 5,
        OpenInterval:     5 * time.Second,
        Adaptive: &hatPeer.ConnectionPoolCircuitBreakerAdaptiveOptions{
            MaxOpenInterval: time.Minute,
            BackoffFactor:   2,
        },
    },
    Dial: dialPeer,
})
```

The pool is one peer target, so the adaptive state is isolated per pool. The
state machine is unchanged: after the threshold is reached, calls are
rejected; after the cooldown, one call is allowed as a probe. A failed probe
doubles the cooldown up to `MaxOpenInterval`. A successful probe closes the
circuit and halves the current cooldown, never below the base interval.
Context cancellation and deadline errors do not count as peer failures and do
not increase the cooldown.

## Measurement

Command:

```text
make benchmark-t-u52
```

AMD Ryzen 9 5950X, Linux/amd64, five benchmark samples, same failing dial
storm workload:

| Mode | Median latency | Memory | Allocations | Dial calls |
| --- | ---: | ---: | ---: | ---: |
| No breaker | 407.1 ns/op | 224 B/op | 4 allocs/op | about 2.9M/sample |
| Fixed breaker | 452.7 ns/op | 224 B/op | 4 allocs/op | 1/sample |
| Adaptive breaker | 430.1 ns/op | 224 B/op | 4 allocs/op | 1/sample |

The adaptive path has no measured allocation or memory increase. Its main
benefit is during a sustained outage: with a five-second base and one-minute
cap, failed probes occur at approximately 5, 15, 35, and 75 seconds, instead
of every five seconds. The tradeoff is slower detection of a peer that recovers
while the interval is backed off; a successful probe halves the interval to
make subsequent recovery responsive. The fixed breaker remains available when
that tradeoff is unacceptable.

## Observability

`ConnectionPool.CircuitBreakerStats()` reports `Adaptive` and
`CurrentOpenInterval` along with the existing state, open, reject, and probe
counters. The stats are a snapshot and do not change breaker behavior.
