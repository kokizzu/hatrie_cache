# CH-U23: Async Queue Registry

Status: implemented as an opt-in library control surface.

## What It Does

`hatPipeline.AsyncBatcherRegistry` gives an application a bounded, named
registry for async insert or other batch queues. It supports:

- registering and unregistering queues;
- deterministic status snapshots sorted by queue name;
- flushing one queue or all registered queues;
- graceful close that drains and closes registered queues; and
- joined, queue-named errors when a multi-queue operation has failures.

`AsyncBatcher` already implements the `AsyncBatcherControl` interface. Other
batcher implementations can implement the same three methods without taking a
dependency on a concrete registry type.

The registry does not start workers, open an HTTP or gRPC endpoint, perform
authentication, or impose queue quotas beyond its bounded registration count.
Those policies remain with the embedding service. This keeps the default
runtime unchanged and avoids creating an unauthenticated management surface.

## Defaults And Limits

| Setting | Default | Hard limit |
| --- | ---: | ---: |
| `MaxQueues` | `1024` | `65536` |
| queue name bytes | n/a | `128` |

`MaxQueues: 0` selects the default. Negative or oversized values are rejected.
Empty and oversized names, nil queue controls, duplicate names, and unknown
names return sentinel errors suitable for `errors.Is`.

## Ownership And Shutdown

`Register` transfers control access to the registry but does not change queue
worker behavior. `Unregister` returns the queue without closing it, so the
caller remains responsible for that queue. `Close` marks the registry closed,
removes all registered queues, and calls `Close(ctx)` on each one. Repeated
`Close` calls observe the first result.

The registry releases its lock before calling queue `Flush`, `Close`, or
`Stats`, so queue implementations can safely call back into application code.
`FlushAll` and `Close` process queue names in sorted order and continue after an
individual error.

## Example

```go
registry, err := hatPipeline.NewAsyncBatcherRegistry(
	 hatPipeline.AsyncBatcherRegistryOptions{MaxQueues: 32},
)
if err != nil {
	 return err
}

queue, err := hatPipeline.NewAsyncBatcher(hatPipeline.AsyncBatcherOptions[Record]{
	 Capacity:     4096,
	 MaxBatchSize: 256,
	 Handler:      insertRecords,
})
if err != nil {
	 return err
}
if err := registry.Register("region-eu", queue); err != nil {
	 queue.Close(context.Background())
	 return err
}

status := registry.Snapshot()
if err := registry.Flush(context.Background(), "region-eu"); err != nil {
	 return err
}
return registry.Close(context.Background())
```

The example intentionally leaves transport and authentication outside the
package. An HTTP, gRPC, or admin-console handler should authenticate before
calling `Snapshot`, `Flush`, `FlushAll`, or `Close`, and should avoid exposing
untrusted queue names without the application's authorization policy.

## Benchmark

The paired benchmark was run with `go test -bench='^BenchmarkCHU23' -benchmem
-count=5` through the repository Makefile on an AMD Ryzen 9 5950X. The direct
batcher baseline and registry benchmark use empty queues and a one-hour flush
interval, so the result isolates control-plane overhead rather than handler or
I/O work.

| Operation | Direct baseline median | Registry median | Time multiplier | Memory / allocs |
| --- | ---: | ---: | ---: | ---: |
| point-in-time `Stats` | `0.75 ns` | n/a | n/a | `0 B / 0` |
| one-queue `Snapshot` | n/a | `146.8 ns` | not equivalent | `120 B / 3` |
| one-queue `Flush` | `604.5 ns` | `669.1 ns` | `1.11x` | `128 B / 2` both |
| 16-queue `FlushAll` | `16 x 604.5 ns` derived | `12.08 us` | `1.25x` per queue derived | `2664 B / 36` |

The snapshot is intentionally more expensive than direct `Stats`: it copies
the registry's named status records and sorts the result for deterministic
operator output. The registry adds little cost to a flush and does not add
allocations to the one-queue flush path in this measurement. It is intended
for administrative control, not for the per-item ingest path.

## Verification

The feature was verified with:

- focused package tests;
- race-enabled package tests; and
- `go vet` for `hat/hatPipeline`.

The tests cover sorted snapshots, pending counts, single and all-queue flush,
duplicate and bounded registration, unregister ownership, graceful draining,
joined errors, context cancellation, repeated close, and closed-registry
rejection.
