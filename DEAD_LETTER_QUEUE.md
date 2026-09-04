# Dead-Letter Queue

hatDataStructure.DeadLetterQueue[T] combines the allocation-free delay queue
with bounded failure retention. It keeps the original deadline and value,
records failure time, attempts, and reason, and supports replaying or
discarding a failure by ID.

~~~go
queue := hatDataStructure.NewDeadLetterQueue[string](128, 256)
queue.EnqueueAfter(time.Now(), time.Second, "refresh")

item, ok := queue.PopReady(time.Now().Add(time.Second))
if ok {
	id := queue.Fail(item, 3, "worker unavailable")
	queue.ReplayAt(id, time.Now())
}
~~~

The zero value supports pending queue operations. Configure retention with
SetDeadLetterLimit; a non-positive limit intentionally retains no failures,
which avoids accidental unbounded memory growth. The queue is not
thread-safe, so callers sharing it must synchronize access.

DeadLetters returns retained failures in failure order. DeadLetter looks up one
ID, ReplayAt moves it back to pending work at an explicit deadline, and
Discard removes it permanently. When the bound is exceeded, the oldest
failures are dropped.

## Measurement

Command: make benchmark-dead-letter-queue.

The benchmark keeps 128 failures resident and compares replay plus re-failure
against an equivalent direct slice and DelayQueue control:

| Implementation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| DeadLetterQueue | 259.1 | 236 | 0 |
| Direct slice control | 224.2 | 211 | 0 |
| Tradeoff | **1.16x higher CPU** | **25 more** | **same** |

This is a control-path cost for replayable failures, not a change to existing
replication persistence or wire formats.
