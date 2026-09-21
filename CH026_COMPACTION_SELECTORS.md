# CH-026 Compaction Selector Policies

The compaction scheduler now has opt-in policies for choosing among queued,
explicitly prioritized tasks. The default remains
`CompactionSelectionPriority`, so existing callers retain the historical
priority-descending, name-ascending order.

## Policies

`CompactionSelectionSizeTiered` groups tasks by the estimated byte count passed
to `ScheduleWithPriorityAndIO`. Smaller size tiers run first, then smaller
estimates, higher explicit priority, older enqueue time, and task name. A zero
estimate is unknown and is ordered after known estimates.

`CompactionSelectionTimeAware` runs the oldest queued task first. Explicit
priority, estimated size, and task name are deterministic tie breakers. This
is useful when a continuously refreshed queue could otherwise keep old work
waiting indefinitely.

The selector only orders work already supplied by the caller. It does not
create a background scheduler, choose merge candidates, or change the storage
engine's compaction algorithm.

## Usage

```go
scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{
	MaxConcurrent:   2,
	SelectionPolicy: hatStorage.CompactionSelectionSizeTiered,
})
if err != nil {
	return err
}

_, err = scheduler.ScheduleWithPriorityAndIO("region-us", 1, 64<<20, compactUS)
if err != nil {
	return err
}
_, err = scheduler.ScheduleWithPriorityAndIO("region-eu", 1, 4<<20, compactEU)
if err != nil {
	return err
}
_, err = scheduler.Run(ctx)
return err
```

Use `CompactionSelectionPriority` or omit `SelectionPolicy` to preserve the
existing behavior. `ScheduleWithPriority` remains valid; its size is simply
unknown to the size-tiered policy.

## Cost and measurement

The default queue representation is unchanged. Selector metadata is stored
only in the explicitly prioritized queue, so ordinary `Schedule` callers do
not pay per-task timestamp or estimate storage. The following five-sample
benchmark used 64 no-op tasks, `MaxConcurrent: 4`, and the same workload for
each policy on Linux/amd64, AMD Ryzen 9 5950X:

| Policy | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative to default |
| --- | --- | ---: | ---: | ---: | ---: |
| Priority (default) | 56,588; 57,244; 57,391; 55,762; 52,219 | 56,588 | 29,162 | 36 | 1.000x |
| Size-tiered | 50,390; 60,492; 57,420; 57,577; 55,983 | 57,420 | 29,161 | 36 | 1.015x slower |
| Time-aware | 57,025; 58,756; 57,115; 56,843; 57,206 | 57,115 | 29,160 | 36 | 1.009x slower |

The policy cost is therefore a small sorting-time difference in this workload,
with no additional measured allocations or bytes per operation. The feature is
opt-in because the workload benefit depends on the caller's compaction shape;
the default does not change.

Run the benchmark with:

```text
make benchmark-ch026-compaction-selector
```
