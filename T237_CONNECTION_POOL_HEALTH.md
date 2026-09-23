# T237: Connection Pool Health Checks

`hat/hatPeer.ConnectionPool` now supports an opt-in idle-connection health
check. This addresses the common case where a peer closes an otherwise idle
socket and the next request would otherwise receive a stale connection.

## API

Set `ConnectionPoolOptions.HealthCheck` when constructing a pool:

```go
pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
	Dial: dialPeer,
	HealthCheck: func(ctx context.Context, connection hatPeer.Connection) error {
		return pingPeer(ctx, connection)
	},
})
```

The default is `nil`, which keeps health checking off. The callback runs only
when an idle connection is acquired. Newly dialed connections are not checked
twice. The callback must honor its context and return an error when the
connection is not usable.

When a check fails, the pool closes the connection, releases its open-capacity
slot, and obtains a replacement through the existing dial retry and backoff
policy. A caller cancellation or deadline is returned directly and does not
cause a replacement dial. `ConnectionPoolStats.HealthChecks` and
`HealthCheckFailures` expose cumulative check counts.

## Operational Guidance

- Keep the probe bounded and cheap, such as a protocol ping or a transport
  health method with the request deadline.
- Use a pool-level timeout in the caller context when a peer can become
  unresponsive.
- Leave the option disabled when the protocol operation itself already
  validates the connection or when the extra round trip is not justified.
- Health checks are lazy; this feature does not start a background goroutine
  or scan idle connections periodically.

## Benchmark

The benchmark reuses one idle connection with `MaxOpen=1` and `MaxIdle=1`.
Results are five samples on AMD Ryzen 9 5950X, with Go benchmark memory
reporting enabled:

| Path | Median ns/op | B/op | Allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Before implementation, health disabled | 54.62 | 0 | 0 | 1.00x |
| After implementation, health disabled | 53.27 | 0 | 0 | 1.03x faster, within noise |
| After implementation, health enabled | 58.92 | 0 | 0 | 1.11x the paired baseline |

The feature has no measured allocation or memory cost. The enabled path costs
about 11.8% CPU in this synthetic reuse loop because it invokes the probe and
updates health counters. The disabled path retains the original acquisition
shape and did not regress in the paired measurements.
