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
- no background goroutine, HTTP route, SQL parser rule, filesystem path
  interpretation, or default scheduler is introduced.

The last point is deliberate. The existing monitoring `/api/storage/compact`
route already performs a synchronous backend-specific range compaction. A
generic SQL or HTTP `OPTIMIZE` command would need a backend-owned target
catalog, authorization policy, and lifecycle integration that this package
cannot infer safely. Callers can expose the controller through their own
authenticated operator surface.

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
| Existing `CompactionScheduler` | 34,521; 29,589; 30,360; 37,445; 36,661 | 34,521 | 17,513 | 35 |
| `CompactionController` | 162,499; 172,362; 153,163; 148,213; 150,976 | 153,163 | 39,453 | 193 |

The controller costs 4.44x CPU, 2.25x measured heap, and 5.51x allocations
on this empty-control benchmark. That is not an optimization of the hot
scheduler path. It is an explicitly opt-in operator/control feature whose
status and bounded-history guarantees account for the cost; direct scheduler
callers retain the lower-overhead path.

## Safety

The controller does not authorize targets or access storage itself. The
callback owner must authenticate the operator, validate the target, enforce
write/maintenance policy, and avoid capturing secrets in the target string or
error. The controller bounds active jobs and retained successful statuses, and
truncates callback errors to 512 bytes.

## Verification

```text
make test-chu35
make test-chu35-package
make race-chu35
make vet-chu35
make benchmark-chu35
make verify-chu35
```
