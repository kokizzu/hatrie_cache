# T249 Queue Capacity, Age, Retry, And Consumer-Lag Metrics

`PriorityVisibilityQueue` now exposes opt-in pressure and consumer-progress
metrics. The option is disabled by default:

```go
queue := hatDataStructure.NewPriorityVisibilityQueueWithOptions[int](
	 hatDataStructure.PriorityVisibilityQueueOptions{
		Capacity:          10_000,
		VisibilityTimeout: time.Minute,
		EnableMetrics:     true,
	},
)

metrics := queue.Metrics(time.Now())
```

`DeduplicatingPriorityVisibilityQueue` forwards the same metrics. The retrying
deduplicating queue returns those fields plus `DeadLetters` and
`MaxAttempts`.

## Fields

| Field | Meaning |
| --- | --- |
| `Enabled` | Whether per-item age and process-local counters are collected. |
| `Capacity` | Configured capacity; `0` means unbounded. |
| `Used` | Pending plus leased items. |
| `Available` | Remaining capacity; `-1` means unbounded. |
| `Pending` | Pending items, including delayed items. |
| `Ready` | Pending items ready at the supplied observation time. |
| `Delayed` | Pending items not ready at the supplied observation time. |
| `Leased` | Items currently hidden by active visibility leases. |
| `OldestReadyAge` | Age of the oldest ready pending item. |
| `OldestLeaseAge` | Age of the oldest active lease. |
| `ConsumerLag` | Alias of the oldest ready item's age. |
| `TotalLeases` | Process-local lease deliveries since construction or restore. |
| `TotalRetries` | Process-local deliveries after the first attempt. |

The `now` argument makes tests deterministic and avoids forcing callers to use
wall-clock time. `Metrics` is non-thread-safe, like the queue itself.

## Persistence

The opt-in flag is preserved through in-memory and binary priority-queue
snapshots. It uses one previously reserved byte in the existing version-2
header; old snapshots with that byte unset remain valid and no per-record wire
size changes. Retry and lease counters are observability counters, so they
reset after restore. Delayed-item age uses its persisted ready time. Immediate
items have no historical enqueue timestamp in the old payload format, so their
age starts at restore; active lease age is reconstructed from the lease
deadline and configured visibility timeout.

## Cost And Measurements

Measurements were run on Linux/amd64, AMD Ryzen 9 5950X, five benchmark
samples per case. The default queue keeps metrics disabled and retains the
existing zero-allocation operation path.

| Benchmark | Raw samples (ns/op) | Median | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Default lease/ack | 176.8, 174.2, 182.1, 174.9, 170.9 | 174.9 | 0 | 0 |
| Metrics-enabled lease/ack | 268.5, 271.2, 273.6, 282.8, 274.2 | 273.6 | 0 | 0 |
| Metrics read, 256 active items | 4070, 4090, 4052, 4151, 4144 | 4090 | 0 | 0 |

The opt-in operation path is `1.56x` the default path. A metrics read is
`O(n)` over pending and leased items and performs no allocations. Retained
heap for 10,000 active leases was measured three times:

| Configuration | Median retained heap | Bytes/item | Relative |
| --- | ---: | ---: | ---: |
| Metrics disabled | 3,070,784 bytes | 307.1 | 1.00x |
| Metrics enabled | 3,517,992 bytes | 351.8 | 1.15x |

The first implementation stored two `time.Time` values per metric entry and
measured `4,130,344` bytes (`1.34x`) and roughly `6,705 ns/op` for a 256-item
metrics read. Packed Unix-nanosecond timestamps reduced that to `1.15x` heap
and `4,090 ns/op`, so the larger representation was discarded.

The feature is therefore deliberately opt-in: monitoring gets queue age and
lag visibility, while users who do not enable it pay only the option checks
and retain the existing no-allocation behavior.
