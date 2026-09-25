# CH-025: Compaction-Pool Priority Policy

Status: partial adoption.

The storage scheduler now supports an opt-in ClickHouse-inspired merge-pool
policy that combines freshness lag and reclaimable space into one deterministic
priority. Existing callers that leave `PriorityPolicy` nil retain the current
scheduler behavior and cost.

## Usage

```go
policy := hatStorage.DefaultCompactionPriorityPolicy()
scheduler, err := hatStorage.NewCompactionScheduler(
    hatStorage.CompactionSchedulerOptions{
        MaxConcurrent:  1,
        PriorityPolicy: &policy,
    },
)

_, err = scheduler.ScheduleWithPriorityMetrics(
    "parts/2026-09-25",
    hatStorage.CompactionPriorityMetrics{
        FreshnessLag:     10 * time.Minute,
        ReclaimableBytes: 32 << 20,
    },
    compact,
)
```

The default policy normalizes freshness in one-minute units and reclaimable
space in one-MiB units, with equal weights. Callers can provide different
units and weights, including a zero weight for one dimension. Both weights
cannot be zero. Scores saturate at the platform's maximum `int` value.

The policy only supplies a priority to the existing bounded scheduler. It does
not create a background worker, change concurrency, or decide whether a part
is eligible for compaction. Merge candidate construction and storage-engine
execution remain caller-owned.

## Safety

- The feature is disabled by default.
- Invalid units and weights are rejected during scheduler construction.
- Negative freshness lag is rejected at scheduling time.
- A configured policy is copied into the scheduler and cannot race with caller
  mutation after construction.
- Existing explicit-priority and unprioritized scheduling APIs are unchanged.

## Benchmark

Command: `make benchmark-ch025-priority`

Machine: AMD Ryzen 9 5950X, linux/amd64. Each sample schedules and drains 64
single-worker tasks. The baseline computes explicit integer priorities in the
caller; the policy path derives the same ordering from freshness and space
metrics.

| Path | Five observed samples (ns/op) | Median | Memory | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Explicit priority baseline | 43,598; 41,543; 43,470; 43,353; 43,115 | 43,353 | 29,448 B, 97 allocs | 1.00x |
| Signal-based policy | 44,168; 44,885; 44,302; 44,798; 45,362 | 44,798 | 29,448 B, 97 allocs | 1.03x |

The policy adds about 3.3% to this control-plane scheduling workload while
adding no measured allocation or memory cost. It is opt-in because this is an
ordering feature, not a free data-plane optimization: users who do not need
freshness/space balancing should keep the policy disabled.

## Verification

```text
make format-ch025-priority
make test-ch025-priority
make test-ch025-package
make race-ch025-priority
make vet-ch025-priority
make benchmark-ch025-priority
```
