# T247: Deduplicating Priority Visibility Queue

T247 adds an opt-in queue type for retryable work identified by a
client-supplied key. `DeduplicatingPriorityVisibilityQueue[T]` wraps the
existing `PriorityVisibilityQueue[T]` scheduling and visibility semantics.

## Semantics

- `Enqueue(key, priority, value)` rejects an empty key, keys larger than 4096
  bytes, duplicate pending work, and duplicate leased work.
- A key remains reserved through `Nack`, delayed retry, and visibility expiry.
- `Ack` or `AckToken` releases the key. The same key can then be enqueued
  again.
- `LeaseWithToken`, `NackToken`, and `AckToken` retain the underlying epoch
  fencing behavior.
- `Snapshot`, `Restore`, `MarshalSnapshot`, `Save`, `Unmarshal`, and `Load`
  preserve client identities as well as queue state. `Load` advances the queue
  epoch, so tokens from before the restart are rejected.
- The wrapper is non-thread-safe, matching the wrapped queue.

```go
queue := hatDataStructure.NewDeduplicatingPriorityVisibilityQueue[string](
    10000,
    time.Minute,
)

// The second enqueue is rejected while "order-42" is pending or leased.
queue.Enqueue("order-42", 10, "send receipt")
queue.Enqueue("order-42", 10, "duplicate receipt")

item, ok := queue.Lease(time.Now())
if ok {
    // Retry with queue.Nack(item.ID, readyAt), or permanently finish it.
    queue.Ack(item.ID)
}
```

Use `SaveDeduplicatingPriorityVisibilityQueue` with an application-owned value
codec for durable checkpoints. Snapshots are CRC-protected and use the same
atomic file publication as the base priority queue.

## Cost Measurement

Command: `make benchmark-t247-deduplicating-queue` (five samples,
`-benchmem`, Linux/amd64, AMD Ryzen 9 5950X). Each iteration enqueues, leases,
and acknowledges one item. The resident case keeps 256 distinct items in the
queue while cycling one item per iteration.

```text
Base empty:       134.8 136.8 132.9 135.3 128.7 ns/op, 0 B/op, 0 allocs/op
Dedup empty:      241.8 243.7 248.3 224.8 251.7 ns/op, 0 B/op, 0 allocs/op
Base resident256: 220.3 213.7 210.3 222.3 212.9 ns/op, 0 B/op, 0 allocs/op
Dedup resident256:373.5 374.5 338.4 351.1 375.0 ns/op, 0 B/op, 0 allocs/op
```

| Workload | Base median | Dedup median | Dedup / base | Hot-path allocations |
| --- | ---: | ---: | ---: | --- |
| Empty | 134.8 ns/op | 243.7 ns/op | 1.81x CPU | 0 B/op, 0 allocs/op |
| 256 resident items | 213.7 ns/op | 373.5 ns/op | 1.75x CPU | 0 B/op, 0 allocs/op |

`make memory-t247-deduplicating-queue` measures retained heap after building
10,000 pending items. Three runs reported approximately 1.165-1.170 MB for
the base queue and 2.024 MB for the deduplicating queue, or about 117 versus
202 bytes per pending item and 1.73x retained heap. The extra memory is the
identity index and reverse lease index; it is the tradeoff that makes duplicate
checks constant-time. The base queue's behavior and cost are unchanged because
this type is opt-in.

The benchmark is a local in-process micro-workload, not a claim about end-to-
end broker throughput. Applications that do not need client-key idempotency
should continue using `PriorityVisibilityQueue`.
