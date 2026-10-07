# CH-U35 Bounded Optimize Control

This is a partial adoption of the ClickHouse-style operator idea of an
explicit, observable optimize/merge operation. `hatStorage.CompactionController`
adds a bounded control layer over the existing `CompactionScheduler`:

- callers submit a stable target, optional priority, estimated I/O bytes, and
  the caller-owned merge callback;
- duplicate active targets are coalesced;
- `Run(ctx)` preserves scheduler concurrency, priority, I/O pacing, and
  cancellation semantics;
- every admitted job has a numeric ID, attempt count, retry state, and a
  bounded error string;
- successful status history is bounded and evicted oldest-first;
- the optional `hatCache.MonitoringOptions` adapter exposes a strict
  `POST /api/storage/optimize` route only when both a controller and a
  caller-owned target resolver are injected;
- no background goroutine, SQL parser rule, filesystem path interpretation,
  or default scheduler is introduced.

The route is deliberately opt-in. The resolver receives the authenticated
request and must authorize the logical target, map it to a callback, and
return a `hatStorage.CompactionRequest`; the monitoring package never
interprets filesystem paths or storage-engine names. The request body is:

```json
{"target":"events/part-0001","priority":10,"estimated_bytes":33554432}
```

The route submits the request and synchronously drains the caller-owned
controller, returning the job snapshot and bounded run counters. The existing
monitoring `/api/storage/compact` route is unchanged.

## Example

```go
controller, err := hatStorage.NewCompactionController(hatStorage.CompactionControllerOptions{
    SchedulerOptions: hatStorage.CompactionSchedulerOptions{
        MaxConcurrent:       1,
        MaxIOBytesPerSecond: 64 << 20,
    },
    MaxPending:      64,
    HistoryCapacity: 64,
})
if err != nil {
    return err
}

job, accepted, err := controller.Submit(hatStorage.CompactionRequest{
    Target:         "events/part-0001",
    Priority:       10,
    EstimatedBytes: 32 << 20,
    Run: func(ctx context.Context) error {
        return mergePart(ctx, "events/part-0001")
    },
})
if err != nil || !accepted {
    return err
}

_, err = controller.Run(ctx)
status, _ := controller.Status(job.ID)
```

`MaxPending` and `HistoryCapacity` use finite defaults of 64. A failed or
canceled callback remains `retry_pending` and is retried by a later `Run`; a
successful job becomes `succeeded`. The controller is opt-in and does not
change direct `CompactionScheduler` behavior.

## Measurement

Five `-benchmem` samples used the same 64-task, no-op callback workload as the
existing scheduler benchmark on Linux/amd64 with an AMD Ryzen 9 5950X. This
isolates control-plane overhead and intentionally excludes real merge I/O.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing `CompactionScheduler` | 25,739; 25,916; 26,058; 26,250; 26,963 | 26,058 | 17,512 | 35 |
| `CompactionController` | 95,168; 95,103; 96,366; 95,222; 95,860 | 95,222 | 39,446 | 193 |

The controller costs 3.65x CPU, 2.25x measured heap, and 5.51x allocations
on this empty-control benchmark. That is not an optimization of the hot
scheduler path. It is an explicitly opt-in operator/control feature whose
status and bounded-history guarantees account for the cost; direct scheduler
callers retain the lower-overhead path.

The HTTP adapter adds its own control-plane cost. Five `-benchmem` samples of
the no-op request path measured 5,489; 5,534; 5,473; 5,846; and 5,766 ns/op,
with a median of 5,534 ns/op, 8,384 B/op, and 45 allocs/op. This includes JSON
decoding, request dispatch, resolver invocation, controller execution, and
JSON encoding; it is not comparable to real merge I/O.

## Safety

The controller does not authorize targets or access storage itself. The
callback owner must authenticate the operator, validate the target, enforce
write/maintenance policy, and avoid capturing secrets in the target string or
error. The controller bounds active jobs and retained successful statuses, and
truncates callback errors to 512 bytes.

## Verification

```text
make test-chu35-optimize-adapter
make race-chu35-optimize-adapter
make vet-chu35-optimize-adapter
make benchmark-chu35-optimize-adapter
make verify-chu35-optimize-adapter
```
