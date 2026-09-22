# M234 Sink Backpressure

## Status

Adopted as an opt-in high/low-watermark admission policy on
`hatReplication.SinkRetryQueue`. M233 already enforces hard count and byte
limits; M234 lets an upstream producer slow down before those hard limits are
reached.

The design combines Materialize-style frontier flow control, Tarantool relay
backpressure, and ClickHouse bounded asynchronous work. It does not start a
worker or block a goroutine. The caller receives `ErrSinkRetryBackpressure`
and decides whether to pause, retry, or route the event elsewhere.

## Defaults And Configuration

Backpressure is disabled by default. Existing M233 behavior therefore remains
unchanged, including its hard `MaxPending` and `MaxBytes` errors.

```go
queue, err := hatReplication.NewSinkRetryQueue(hatReplication.SinkRetryOptions{
    Source:     "orders",
    MaxPending: 1024,
    MaxBytes:   64 << 20,
    Backpressure: hatReplication.SinkRetryBackpressureOptions{
        Enabled:     true,
        HighPending: 800,
        LowPending:  400,
    },
})
```

When enabled, omitted high watermarks default to 80% of the configured hard
limit. Omitted low watermarks default to half of the resolved high watermark.
Pressure starts when either pending count or pending bytes reaches its high
watermark. It clears only when both dimensions are at or below their low
watermarks, preventing rapid admit/reject oscillation.

The queue reports `Backpressured` and `BackpressureEvents` in `Stats`, and
`BackpressureStatus` exposes the resolved thresholds for monitoring. The
status counter is process-local telemetry, while restored queue occupancy is
reevaluated against the configured thresholds on restart.

## Admission Semantics

- A new output identity is rejected with `ErrSinkRetryBackpressure` while the
  queue is throttled.
- An identical duplicate still returns `SinkRetryDuplicate`.
- A newer pending record for the same output identity still compacts the old
  record; an in-flight record is never replaced.
- `Ack` can release pressure once both low-watermark conditions are satisfied.
- The hard queue bounds remain authoritative even when backpressure is off.

This keeps compaction and recovery possible during an outage without allowing
unbounded new identities to accumulate. The queue remains thread-safe and
uses the same mutex as M233; no additional goroutine, channel, or per-record
allocation is introduced for the controller.

## Tradeoff Measurement

Five benchmark samples were run on Linux amd64, AMD Ryzen 9 5950X. Each
enqueue benchmark performs enqueue, claim, and acknowledgement for one record.

| Workload | Default-off median | Enabled healthy median | Result | B/op | allocs/op |
| --- | ---: | ---: | --- | ---: | ---: |
| Enqueue + claim + ack | 431.3 ns/op | 424.7 ns/op | 0.98x enabled/off; within run variance | 224 both | 6 both |
| Reject a new identity while throttled | n/a | 35.23 ns/op | allocation-free admission signal | 0 | 0 |

The enabled healthy path did not add memory allocation and was not measurably
slower in the paired runs. The rejection path is intentionally much cheaper
than serializing and retaining another output record. The tradeoff is that
enabled callers must handle a new retryable error and choose their upstream
policy.

## Verification

- `make test-m234-sink-backpressure`
- `make test-m234-sink-backpressure-package`
- `make race-m234-sink-backpressure`
- `make vet-m234-sink-backpressure`
- `make benchmark-m234-sink-backpressure`
