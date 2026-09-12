# CH-027 Compaction Scheduler Observability

The compaction scheduler now reports the oldest pending and running task
timestamps without changing scheduling order, concurrency, retry behavior, or
the existing `Stats()` result shape.

```go
ages := scheduler.Ages()
pendingAge := ages.OldestPendingAge(time.Now())
runningAge := ages.OldestRunningAge(time.Now())
```

`Ages()` is a read-only snapshot. It returns Unix nanosecond timestamps so a
caller can use its own clock, test clock, or metrics scrape timestamp. Zero
means there is no pending or running task. The age helpers clamp clock
regressions to zero and do not read the wall clock themselves.

This is generic queue/run telemetry inspired by ClickHouse background-task
observability. Storage-engine-specific bytes, compaction age policy, and TTL
metrics remain the responsibility of the provider that schedules the task.

The default `Stats()` method remains allocation-free and keeps its original
fields. Timestamp capture occurs only when the first task enters a pending
queue or a run starts; duplicate/coalesced schedules do not capture additional
timestamps.
