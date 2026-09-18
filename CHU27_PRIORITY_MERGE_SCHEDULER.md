# CH-U27: Priority Merge Scheduler

Hatrie Cache now supports opt-in priorities for `hatStorage.CompactionScheduler`.
This adopts the ClickHouse-style idea of giving latency-sensitive background
merge work an explicit scheduling preference while keeping the existing
zero-value scheduler behavior unchanged.

```go
queued, err := scheduler.ScheduleWithPriority("orders-hot", 100, compactHot)
```

Higher values run first. Equal priorities retain deterministic task-name
ordering. A duplicate queued task is still coalesced; a later higher-priority
duplicate raises its queued priority without replacing its callback. Failed
tasks retain their priority when requeued for a later `Run`.

`Schedule` remains the default-priority API. It does not allocate priority
metadata, and callers that do not use `ScheduleWithPriority` retain the
existing queue layout and ordering.

## Tradeoff

The priority path intentionally performs extra queue bookkeeping and ordering.
The synthetic no-op benchmark below schedules and drains 64 tasks with one
worker, so it exaggerates that control-plane cost compared with real merge
callbacks:

| Path | Median ns/op | B/op | allocs/op | Relative time |
|---|---:|---:|---:|---:|
| Existing/default scheduling | 33,495 | 17,704 | 96 | 1.00x |
| Explicit priority scheduling | 45,077 | 20,376 | 98 | 1.35x |

The default path was also compared before and after the change using the
existing scheduler benchmark. The 64-task median remained within run variance
(`21,542` to `21,860 ns/op`) and retained `3,400 B/op` and `12 allocs/op` in
both runs. Priority scheduling is therefore off unless the caller explicitly
accepts its bounded control-plane cost for better latency ordering.

Run the measurements with:

```text
make benchmark-chu27-priority-baseline
make benchmark-chu27-priority
```
