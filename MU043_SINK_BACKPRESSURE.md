# Sink Backpressure Catalog

M-U43 adds an opt-in, transport-neutral backpressure catalog to
`hatPipeline`. It tracks emitted and acknowledged sink frontiers, exposes a
bounded lag state, and lets upstream producers wait while a sink is above its
high watermark.

## Use

```go
registry, err := hatPipeline.NewSinkBackpressureRegistry(
    hatPipeline.SinkBackpressureRegistryOptions{},
)
if err != nil {
    return err
}
if err := registry.Register("orders", hatPipeline.SinkBackpressureSinkOptions{
    HighWatermark: 4096,
    LowWatermark:  2048,
}); err != nil {
    return err
}

if err := registry.WaitUntilWritable(ctx, "orders"); err != nil {
    return err
}
batch, err := source.Next(ctx)
if err != nil {
    return err
}
if err := sink.Write(ctx, batch); err != nil {
    return err
}
return registry.Record("orders", batch.EmittedFrontier, batch.AcknowledgedFrontier)
```

`Advance` and `Acknowledge` are available when the two frontiers arrive in
separate callbacks. `Record` updates both atomically. The registry does not
own a queue, transport connection, goroutine per sink, or row payload; callers
place `WaitUntilWritable` before their existing `AsyncBatcher.Submit` or sink
write path and report frontiers from the sink's acknowledgement callback.

## Watermarks

- A lag at or above `HighWatermark` sets `Blocked=true`.
- A blocked sink remains blocked until lag is at or below `LowWatermark`.
- The default high watermark is 1024 and the default low watermark is 512.
- A sink-specific high watermark with no low watermark derives its low
  watermark as half the sink-specific high watermark.
- Emitted and acknowledged frontiers are monotone; acknowledgement cannot be
  ahead of emitted progress.
- Waiters are cancellable and wake on acknowledgement, unregister, close, or
  context cancellation. Registry close rejects later updates.

The catalog is deliberately opt-in. Existing queues, SQL sink progress, and
ordinary pipeline behavior remain unchanged until a caller constructs and
connects the registry.

## Operational Safety

The registry retains only sink names, two uint64 frontiers, watermarks, a
blocked bit, and an update timestamp. It never stores row data or error text.
Sink names are trimmed and empty names are rejected. Watermark values are
bounded to `1<<62`. Snapshot output is sorted by sink name for stable metrics
and operator output. `WaitUntilWritable` has no unbounded goroutine or waiter
allocation; each sink has one notification channel.

## Verification And Benchmark

```sh
make test-mu43
make verify-mu43
make benchmark-mu43-baseline
make benchmark-mu43
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X. The direct baseline is only
frontier subtraction; it excludes locking, state validation, timestamps, and
notifications, so it is a lower-bound cost rather than an equivalent feature.

| Workload | Median ns/op | B/op | Allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Direct frontier arithmetic baseline | 0.2484 | 0 | 0 | lower-bound control |
| Registry `Record` | 60.05 | 0 | 0 | 242x the arithmetic-only control |
| Registry `Advance` + `Acknowledge` | 119.9 | 0 | 0 | two synchronized updates |
| Ready `WaitUntilWritable` | 15.19 | 0 | 0 | no blocking or allocation |
| `Snapshot` of 128 sinks | 20,056 | 12,456 | 4 | diagnostic/catalog path |

The result is a correctness and operability feature, not a raw arithmetic
optimization. Its hot update and ready-wait paths add no heap allocations;
the measured synchronization cost is explicit. Raw samples are reproducible
with the Makefile targets above.
