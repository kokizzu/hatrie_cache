# TG43 Consumer-Group Queue

`hatPipeline.ConsumerGroupQueue[T]` composes two existing primitives:

- `ConsumerGroupFence` owns partition membership and generation fencing.
- `hatDataStructure.VisibilityQueue[T]` owns ready items, visibility leases,
  acknowledgement, retry, and epoch-fenced queue tokens.

This is a local, opt-in queueing primitive for Tarantool-style consumer-group
workflows. It does not perform cluster membership, network coordination, or
replication. A shared coordinator must publish the same assignment generation
to every process that consumes a group.

## Configuration

```go
queue, err := hatPipeline.NewConsumerGroupQueue[string](hatPipeline.ConsumerGroupQueueOptions{
	Group:                "orders",
	PartitionCount:       2,
	CapacityPerPartition: 1024,
	VisibilityTimeout:    30 * time.Second,
	Epoch:                1,
})
```

Defaults follow `VisibilityQueue`: zero capacity means unbounded, zero timeout
uses the standard visibility timeout, and zero epoch uses the standard queue
epoch. `PartitionCount` is required and must be positive. Negative capacity is
rejected instead of silently becoming unbounded.

The queue is not thread-safe. Serialize calls per queue, or put a caller-owned
lock around the queue when multiple workers share it.

## Consume, Acknowledge, And Retry

```go
_, err = queue.Rebalance([]hatPipeline.ConsumerGroupPartitionOwner{
	{Partition: 0, Member: "worker-a"},
	{Partition: 1, Member: "worker-b"},
})
if err != nil {
	panic(err)
}

queue.Enqueue(0, "order-123")
now := time.Now()
lease, err := queue.Lease("worker-a", 0, now)
if err != nil {
	// ErrConsumerGroupQueueEmpty means there is no ready item. Fence errors
	// mean membership or generation ownership is invalid.
	return err
}

if err := process(lease.Value); err != nil {
	return queue.Nack(lease, now.Add(time.Second))
}
return queue.Ack(lease)
```

`Lease` checks the member and current generation before taking an item. `Ack`
and `Nack` validate that same lease immediately before the queue side effect.
The returned `QueueToken` is embedded in the lease and is checked by the
underlying visibility queue, so duplicate or unknown acknowledgements fail
with `ErrConsumerGroupQueueLeaseInvalid`.

`Attempts` starts at one and increases after a nack or visibility expiration.
`PendingLen`, `LeaseLen`, and `Len` expose per-partition and total pressure for
backpressure and operational metrics.

## Rebalance Safety

Every successful `Rebalance` increments the consumer-group generation. A lease
from the previous generation cannot acknowledge or retry after ownership
changes; the call returns `ErrConsumerGroupFenceStaleGeneration`. The item is
not discarded by that fencing failure. It becomes available to the new owner
when its visibility timeout expires, which prevents an old owner from deleting
work while allowing the new owner to recover it.

An invalid assignment is rejected before the current generation is changed.
Partitions outside `PartitionCount` return
`ErrConsumerGroupQueuePartitionInvalid`; duplicate partitions, invalid member
names, and other assignment errors retain the underlying fence errors.

## Performance Tradeoff

The wrapper is deliberately opt-in, so existing `ConsumerGroupFence` and
`VisibilityQueue` callers have no new branch or allocation. On an AMD Ryzen 9
5950X, linux/amd64, five `-benchmem` samples measured the steady-state
lease/ack/re-enqueue loop as follows:

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Manual fence + visibility queue | 117.7, 119.4, 121.7, 120.1, 122.6 | 120.1 | 0 | 0 |
| `ConsumerGroupQueue` | 134.5, 131.4, 138.4, 132.7, 131.6 | 132.7 | 0 | 0 |

The typed wrapper is therefore `1.11x` the CPU cost of the equivalent manual
composition in this run, with no steady-state heap or allocation increase. The
CPU cost buys one API that cannot accidentally omit the ownership validation or
queue-token check. It is not a replacement for the lower-level primitives in
latency-critical code that already composes them correctly.

Reproduce with:

```text
make benchmark-m093-consumer-group
```
