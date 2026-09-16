# MZ-07 Frontier-Aware Source Backpressure

This is an opt-in `hatPipeline` primitive inspired by Materialize's frontier
based source coordination. It prevents a producer from admitting a frontier
more than `MaxLag` ahead of the consumer frontier. The gate stores only
frontier counters and does not retain, copy, or inspect source records.

## Default And Scope

The feature is disabled by default. There is no automatic SQL, journal, or
connector integration. A caller opts in by constructing a
`hatPipeline.FrontierBackpressure` and checking admission before it publishes
work to its own bounded queue or downstream operator.

`MaxLag: 0` selects the sane default of `1024` frontier units. This default is
used only after a caller has explicitly constructed the gate; it does not turn
backpressure on for existing pipelines.

The limit is measured in the caller's frontier domain, not rows, bytes, or
time. The caller must choose a frontier domain whose lag is meaningful and
must connect admission to its own buffering. The gate cannot bound data that
was read or queued before admission.

## API

```go
gate, err := hatPipeline.NewFrontierBackpressure(
	 hatPipeline.FrontierBackpressureOptions{MaxLag: 128},
)
if err != nil {
	return err
}

// Nonblocking producer path.
admitted, err := gate.TryAdmit(sourceFrontier)
if err != nil {
	return err
}
if !admitted {
	return ErrSourceWouldOutrunConsumer
}

// Consumer path, when waiting is preferable to rejection.
if err := gate.Wait(ctx, sourceFrontier); err != nil {
	return err
}
if err := downstream.Consume(batch); err != nil {
	return err
}
return gate.AdvanceConsumed(sourceFrontier)
```

`TryAdmit` and `Wait` update the observed producer frontier only after
admission. Repeated or older admitted frontiers are accepted without moving
the stored producer frontier backward. `AdvanceConsumed` is monotone and
rejects regressions with `ErrFrontierBackpressureRegression`. A consumer may
advance beyond the latest producer frontier, which is useful for empty input
batches.

`Wait` is cancellation-aware. `Close` is idempotent, permanently rejects new
work with `ErrFrontierBackpressureClosed`, and wakes blocked waiters. The
following errors are available for explicit handling:

| Error | Meaning |
| --- | --- |
| `ErrFrontierBackpressureNil` | A method was called on a nil gate. |
| `ErrFrontierBackpressureContextNil` | `Wait` received a nil context. |
| `ErrFrontierBackpressureClosed` | The gate was closed. |
| `ErrFrontierBackpressureRegression` | The consumer frontier moved backward. |

`Stats` exposes `Produced`, `Consumed`, `Lag`, successful `Admitted` calls,
`BlockedAttempts`, `Blocked`, `Closed`, and the configured `MaxLag`. `Blocked`
means a `Wait` caller is currently parked; a rejected `TryAdmit` does not make
the gate retain the rejected frontier.

## Memory And Concurrency

The ordinary `TryAdmit`, admitted `Wait`, `AdvanceConsumed`, and `Stats` paths
use fixed state and perform zero heap allocations after construction. The
gate uses one mutex per instance and a close-and-replace notification channel
only when a consumer advance or close must wake a waiter. Use separate gates
for independent sources when a single hot gate would create contention.

This is a coordination boundary, not a queue. It limits future admission but
does not reclaim already buffered records, persist offsets, provide a
distributed lease, or guarantee exactly-once delivery. Those responsibilities
remain with the source and downstream pipeline.

## Measurement

Run `make benchmark-mz007-frontier-backpressure`. The five-sample run below
used Go's `-benchmem`, `-benchtime=200ms`, and an AMD Ryzen 9 5950X Linux/amd64
host. The baseline is a no-lock, no-state-update admission function with the
same frontier arithmetic; it is a lower-bound control, not an end-to-end
source benchmark.

| Operation | Five raw samples (ns/op) | Median | B/op | Allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| No-lock arithmetic control | 1.683, 1.670, 1.672, 1.873, 1.875 | 1.683 | 0 | 0 | 1.00x |
| Admitted `TryAdmit` | 6.336, 6.512, 6.133, 6.371, 6.427 | 6.371 | 0 | 0 | 3.79x |
| Admitted `Wait` | 5.398, 5.346, 5.404, 5.373, 5.356 | 5.373 | 0 | 0 | 3.19x |
| Rejected `TryAdmit` | 6.500, 6.297, 5.985, 6.360, 6.438 | 6.360 | 0 | 0 | 3.78x |

The feature therefore adds about 4 ns over the arithmetic-only control while
keeping the hot path allocation-free. The intentional tradeoff is reduced
source throughput when the consumer falls behind; in exchange, a correctly
wired caller prevents an unbounded lagging-input queue. Blocking and wake-up
latency are scheduling-dependent and are covered by the focused and race
tests rather than represented by a misleading nanosecond loop benchmark.

## Verification

```text
make test-mz007-frontier-backpressure
make verify-mz007-frontier-backpressure
make benchmark-mz007-frontier-backpressure
```

The verification target runs focused tests, the complete `hatPipeline` package,
race tests for both the focused tests and the complete package, and `go vet`.
