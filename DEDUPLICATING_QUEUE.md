# Deduplicating Queue

`hatDataStructure.DeduplicatingQueue[K, T]` rejects a second enqueue while the
same client key is active. The first payload wins; the key becomes eligible for
enqueue again only after `Dequeue` or `Clear`.

```go
queue := hatDataStructure.NewDeduplicatingQueue[string, Task](256)
if queue.Enqueue("job-42", task) {
    // accepted
}
queue.Enqueue("job-42", replacement) // false; original task remains queued
key, task, ok := queue.Dequeue()
```

The queue is FIFO, keeps an active-key map, releases dequeued key/value
references, and compacts consumed prefixes. It is not synchronized and has no
implicit TTL or durable completed-key registry; callers that need retry-window
deduplication should retain identities outside the queue or use a higher-level
policy. The initial capacity is only a preallocation hint, so callers should
enforce an application-specific queue limit when keys are untrusted.

## Measurement

Five isolated `-benchmem` samples submitted 100,000 tasks with 100 repeating
client identities on an AMD Ryzen 9 5950X.

| Workload | Plain FIFO | Deduplicating queue | Relative result | Heap / allocs |
| --- | ---: | ---: | --- | --- |
| Admission and dequeue only | 156,440 ns/op | 1,077,623 ns/op | 6.9x CPU cost | 802,820 / 6,232 B/op; 1 / 5 allocs |
| Admission plus 64-step task work | 5,959,487 ns/op | 1,069,376 ns/op | 5.6x faster | 802,816 / 6,232 B/op; 1 / 5 allocs |

The feature is therefore an explicit workload policy, not a faster replacement
for a queue when every task is unique and useful. It pays a map lookup to avoid
repeating work and retains only the active unique tasks in the duplicate-heavy
case.
