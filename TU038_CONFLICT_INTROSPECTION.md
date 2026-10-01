# T-U38 Conflict Introspection Stream

`hatReplication.ConflictEventLog` is an opt-in, bounded in-memory cursor
stream for diagnosing conflict-policy decisions. It records redacted metadata
after `ConflictPolicyRegistry.Resolve` runs:

- space name and conflict-policy mode;
- left and right version coordinates;
- selected winner coordinates when a policy resolves the conflict;
- `resolved`, `rejected`, or `error` outcome;
- a monotonic sequence number for cursor reads.

Application keys, values, and payloads are never copied into an event. The
space name and node IDs are still metadata, so access to the event stream
should follow the operator's normal replication-observability controls.

## Configuration

The default is disabled. A registry with no event log keeps the existing
resolution path and allocation behavior. Enable it explicitly when a bounded
diagnostic history is useful:

```go
log, err := hatReplication.NewConflictEventLog(1024)
if err != nil {
    return err
}
registry.SetEventLog(log)
defer log.Close()
```

Capacity `0` selects `DefaultConflictEventCapacity` (`1024`). Capacities must
be between `1` and `MaxConflictEventCapacity` (`65536`). `SetEventLog(nil)`
disables the observer again.

## Reading and lifecycle

`Read(after, limit)` returns detached events with sequence numbers greater than
`after`; a non-positive limit reads all retained events. A cursor older than
the retained ring returns `ErrConflictEventCursorExpired`, so consumers must
resnapshot or advance their checkpoint after retention loss.

`Wait(ctx, after)` blocks until a newer event is available, the cursor expires,
the log closes, or the context is canceled. `Stats()` reports capacity,
retained count, dropped count, and the first/last retained sequence without an
allocation. `Close()` stops new events and wakes waiters while allowing already
retained events to be read.

The stream is intentionally in-memory. Applications that need durable audit
history can read the bounded stream and persist a redacted representation in
their own controlled sink.

## Measurement

Measurements use `make benchmark-t-u38-baseline` and
`make benchmark-t-u38` on an AMD Ryzen 9 5950X. Each row is five `-benchmem`
samples; the median is shown.

| Path | Five samples (ns/op) | Median | Bytes/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing registry default, before observer | 17.68, 18.25, 17.81, 18.32, 17.56 | 17.81 | 0 | 0 |
| Existing registry default, after observer support | 17.39, 18.02, 18.14, 17.73, 17.70 | 17.73 | 0 | 0 |
| Dedicated observer benchmark, disabled | 11.68, 12.58, 12.63, 12.46, 12.36 | 12.46 | 0 | 0 |
| Dedicated observer benchmark, redacted log enabled | 74.99, 76.61, 73.69, 76.77, 75.48 | 75.48 | 112 | 1 |

The feature is not a default-path speedup. Its cost is deliberately paid only
when the observer is enabled: about 63 ns/op and 112 bytes per recorded event
relative to the dedicated disabled path in this run. The ring is bounded and
has no background goroutine; the default remains disabled and allocation-free.

Focused package tests, the race test, and vet cover the feature. See
[BENCHMARK.md](BENCHMARK.md#t-u38-conflict-introspection-stream) for the raw
benchmark context.
