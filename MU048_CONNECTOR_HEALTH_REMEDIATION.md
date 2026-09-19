# M-U48 Source Connector Health Remediation

M-U48 adds an opt-in health control plane for `hatPipeline.ConnectorRegistry`.
It provides bounded synchronous retries, context-aware exponential backoff,
observable health status, and explicit quarantine/release. Existing lifecycle
methods do not retry or start background goroutines.

## API

```go
policy := hatPipeline.DefaultConnectorHealthPolicy()
status, err := registry.StartWithHealthPolicy(ctx, "orders", policy)
if err != nil {
	// status.LastError retains the connector error; err may be context.Canceled.
	return err
}

status, err = registry.QuarantineConnector(ctx, "orders", "upstream revoked access")
if err != nil {
	return err
}
_ = status

if err := registry.ReleaseConnectorQuarantine("orders"); err != nil {
	return err
}
```

`StartWithHealthPolicy` starts `Created` or `Failed` connectors, resumes
paused connectors, and returns an already-running connector as healthy. A
failure can be retried when `Retryable` accepts it. `QuarantineOnFailure`
records the final failure as `ConnectorQuarantined`; otherwise it records
`ConnectorHealthFailed`. Quarantine blocks later remediation until an
explicit release. Quarantine pauses a running connector before recording the
quarantine, while release does not start the connector automatically.

`HealthStatus` and `HealthSnapshot` expose `Unknown`, `Healthy`, `Retrying`,
`ConnectorHealthFailed`, and `ConnectorQuarantined` states. Error/reason text
is capped at 4 KiB; attempts are capped at 128 and backoff at one hour.

## Defaults and safety

`DefaultConnectorHealthPolicy` uses three total attempts, 100 ms initial
backoff, a 5 s maximum backoff, and quarantine after failure. The policy is
still inert until the caller invokes `StartWithHealthPolicy`. Numeric zero
values select those numeric defaults; `QuarantineOnFailure` must be explicitly
set when constructing a custom policy. Context cancellation stops the timer
without another attempt and preserves the last connector error in health
status.

## Benchmark

Machine: AMD Ryzen 9 5950X, linux/amd64. Five `-benchmem` samples. The
baseline creates and registers the same connector before calling direct
`Start`; this measures control-plane overhead, not connector I/O.

| Operation | Median ns/op | B/op | allocs/op | Relative to direct Start |
| --- | ---: | ---: | ---: | --- |
| Direct lifecycle `Start` | 1,072 | 3,240 | 7 | baseline |
| Health policy, immediate success | 1,497 | 4,264 | 8 | 1.40x time, 1.32x bytes, +1 alloc |
| Health policy, one retry | 2,386 | 4,528 | 12 | 2.23x time, 1.40x bytes, +5 allocs |

Raw median inputs were direct `Start`: 986.6, 1,072, 1,075, 1,082, 1,048
ns/op; policy success: 1,428, 1,487, 1,499, 1,497, 1,608 ns/op; and one
retry: 2,360, 2,453, 2,439, 2,386, 2,275 ns/op.

This is an explicit resilience tradeoff paid only by callers that request
health remediation. It is not a claim that a retry is faster than a direct
start; the benefit is bounded recovery and operator quarantine without
changing ordinary connector behavior.
