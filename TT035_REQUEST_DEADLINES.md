# TT-035 Request Deadlines

Synchronous command requests can now use one optional timeout consistently across
the HTTP monitoring API and the native gRPC API.

## Configuration

The CLI flag is:

```text
-command-request-timeout=2s
```

The default is `0`, which disables the server-configured timeout and preserves
the caller's existing context deadline. The same value is passed to
`MonitoringOptions.RequestTimeout` and `CacheGRPCOptions.RequestTimeout` for
embedded users.

Negative values are rejected during configuration validation.

## Scope

- Unary HTTP and gRPC commands receive the configured maximum duration.
- Ordered and multiplexed gRPC command streams create a fresh timeout for every
  command message, so one slow request does not consume the next request's
  budget.
- The derived context is propagated through quorum and replication paths.
- An earlier caller deadline always wins.
- A local in-memory command is still atomic and is not forcibly interrupted
  halfway through execution; the deadline governs context-aware work around it.

## Cost

The default-off path returns the original context and a no-op cancellation
function, so it does not add a timer or context allocation. An enabled timeout
adds the standard Go timer context per command. It does not change command
serialization, response bandwidth, storage format, or replication payloads.

The reproducible benchmark is:

```text
make benchmark-tt035-request-deadlines
```

The target reports disabled and enabled helper costs with `-benchmem`, plus the
existing HTTP command benchmark for a before/after path comparison. Results are
recorded in `BENCHMARK.md` after verification.
