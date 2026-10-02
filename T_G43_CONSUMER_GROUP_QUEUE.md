# Tarantool-Inspired Consumer Group Queue

`hatDataStructure.ConsumerGroupQueue[T]` is an opt-in work-queue layer for
named consumer groups. It reuses `VisibilityQueue[T]` for pending items,
leases, expiry ordering, and retry storage.

## Semantics

- Each group has its own work stream and capacity.
- Consumers registered in one group compete for each item; an item is leased
  to only one consumer at a time.
- A consumer in another group cannot see the first group's items.
- `Ack` completes a delivery, while `Nack` returns it for a later attempt.
- Expired leases are requeued by `Lease` and by explicit `RequeueExpired`.
- `Unregister` immediately releases that consumer's active leases.
- Lease tokens include queue epoch, group, consumer, item ID, and attempt. A
  wrong consumer, stale retry, or previous queue epoch cannot acknowledge a
  current lease.
- The type is intentionally non-thread-safe, matching `VisibilityQueue`.

```go
queue := hatDataStructure.NewConsumerGroupQueue[string](1024, 30*time.Second)
queue.Register("workers", "worker-a")
queue.Register("workers", "worker-b")
queue.Enqueue("workers", "job-42")

lease, ok := queue.Lease("workers", "worker-a", time.Now())
if ok {
	queue.Ack(lease.Token)
}
```

The queue is not a replacement for the existing visibility queue. It adds
ownership and retry fencing only when callers need consumer groups; existing
queue users pay no runtime or storage cost.

## Benchmark

Machine: AMD Ryzen 9 5950X, Linux/amd64. Each value is the median of five
`go test` samples with `-benchtime=200ms -benchmem`.

| Workload | Existing `VisibilityQueue` | `ConsumerGroupQueue` | Relative cost | Allocations |
|---|---:|---:|---:|---:|
| One item, lease then ack | 90.06 ns/op, 0 B/op | 201.2 ns/op, 0 B/op | 2.23x CPU | 0 vs 0 |
| 256 resident items, lease/ack | 541.8 ns/op, 0 B/op | 704.5 ns/op, 0 B/op | 1.30x CPU | 0 vs 0 |
| Construct queue and one group | 2,000 ns/op, 16,384 B/op | 2,352 ns/op, 16,872 B/op | 1.18x CPU, +488 B | 1 vs 8 |

The additional CPU and setup allocations are the explicit cost of group maps,
consumer ownership checks, and retry-token fencing. The resident workload
does not scan all active owners on every lease; stale owner metadata is cleaned
on expiry/release paths and bounded lazily after mass expiry.

## Verification

```text
make test-consumer-group-queue
make test-all-consumer-group-queue
make race-consumer-group-queue
make vet-consumer-group-queue
make benchmark-consumer-group-queue
```

The focused tests cover group isolation, single delivery, wrong-owner
rejection, retry-attempt fencing, expiry recovery, and release during consumer
unregistration.
