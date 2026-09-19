# Connector Health Remediation

MU-048 adds an opt-in retry and quarantine policy to `hat/hatPipeline` connector startup.
The existing `ConnectorRegistry.Start` lifecycle remains unchanged. Callers that need
bounded remediation can use `StartWithHealthPolicy`.

## Use

```go
policy := hatPipeline.ConnectorHealthPolicy{
	MaxAttempts:     4,
	QuarantineAfter: 3,
	InitialBackoff: 25 * time.Millisecond,
	MaxBackoff:     500 * time.Millisecond,
}

result, err := registry.StartWithHealthPolicy(ctx, "orders", policy)
switch {
case err == nil && result.Recovered:
	// The connector failed transiently and then recovered.
case errors.Is(err, hatPipeline.ErrConnectorHealthQuarantined):
	// Keep the connector failed and alert or require operator intervention.
case err != nil:
	// The context, registry, or connector returned a non-retryable error.
}
```

The policy call is caller-driven. It does not create a background goroutine or
change the default `Start` path, so deployments can choose their own scheduler,
alerting, and operator override behavior.

## Defaults And Bounds

Zero-valued policy fields use these defaults:

| Field | Default |
| --- | ---: |
| `MaxAttempts` | `3` |
| `QuarantineAfter` | `MaxAttempts` |
| `InitialBackoff` | `100ms` |
| `MaxBackoff` | `2s` |

The implementation accepts at most `1024` attempts and a maximum backoff of one
hour. Negative values, a quarantine threshold greater than the attempt limit, or
an initial backoff greater than the maximum are rejected before startup.

`MaxAttempts` includes the first startup attempt. A connector is quarantined
after `QuarantineAfter` retryable callback failures. Quarantine returns an error
wrapping `ErrConnectorHealthQuarantined` and leaves the connector in the existing
`Failed` state; it does not delete offsets or unregister the connector.

## Retry Rules

Connector callback failures are retried with bounded exponential backoff. Context
cancellation and registry/lifecycle errors are returned immediately. A canceled
context also interrupts an in-progress backoff timer. The result reports total
attempts, callback failures, recovery, and quarantine so callers can emit their
own metrics without inspecting internal state.

## Benchmark

The benchmark uses a fresh registry and connector for every iteration. It was run
with Go benchmarks, `-benchmem`, and `-count=5` on an AMD Ryzen 9 5950X.

| Path | Samples (ns/op) | Median | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Clean baseline | 466.0, 436.5, 415.9, 425.1, 435.5 | 435.5 | 544 | 5 |
| Existing `Start` | 430.8, 426.8, 437.4, 425.4, 407.0 | 426.8 | 544 | 5 |
| `StartWithHealthPolicy` | 416.6, 411.7, 388.1, 415.4, 425.2 | 415.4 | 544 | 5 |

The samples overlap normal benchmark noise; this feature is not presented as a
throughput optimization. The existing start path keeps the same measured memory
profile, and the policy path adds no measured allocations in the successful
single-attempt case. Failed attempts intentionally pay for the configured timer
and backoff, which is the tradeoff for avoiding retry storms.

## Verification

```text
make test-mu048
make verify-mu048
make benchmark-mu048
```

The verification target runs package tests, a focused race test, and `go vet`.
