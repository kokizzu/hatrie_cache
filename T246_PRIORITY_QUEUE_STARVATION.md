# T246 Priority Queue Starvation Bounds

`hatDataStructure.PriorityVisibilityQueue` normally uses strict priority
ordering: lower numeric priorities are leased first and equal priorities are
FIFO. That is predictable and fast, but a continuous stream of high-priority
work can indefinitely delay older low-priority work.

## Configuration

```go
queue := hatDataStructure.NewPriorityVisibilityQueue[string](0, time.Minute)
queue.SetStarvationBound(16)

bound := queue.StarvationBound() // 16
```

The default is `0`, which disables bounded fairness and preserves the original
strict-priority behavior. A non-positive value disables the feature and resets
the current fairness burst.

When the bound is enabled and more than one item is ready:

1. Up to `bound` leases use the normal priority/FIFO heap.
2. The next lease selects the oldest ready item by insertion sequence,
   regardless of priority.
3. The fairness burst resets and the cycle starts again.

The bound counts ready-item lease operations, not elapsed time. Delayed items
do not participate until they become ready, and active leases do not count as
ready work. A queue with zero or one ready item resets the burst because there
is no competing ready work to starve.

For example, with a bound of `2`, one low-priority item followed by a stream
of high-priority items is served as:

```text
high, high, oldest-low, high, high, oldest-low, ...
```

## Persistence And Cost

The in-memory snapshot includes `StarvationBound` and `StarvationBurst`, so a
checkpoint/restore continues the same fairness cycle. Binary snapshots use
format version 2 and add eight header bytes. Version 1 snapshots remain
readable and restore with fairness disabled, preserving their old semantics.

The queue stores two `uint32` values per queue, not per item. The default path
continues to use the existing heap and reports zero allocations in the
benchmark. Bounded fairness is deliberately opt-in: when the bound is
reached, selecting the oldest ready entry scans the ready slice, so its CPU
cost grows with the number of ready items. The measured 1,024-item workload
was 375.4 ns/op with bound 16 versus 273.4 ns/op with strict priority, with
zero allocations in both cases. Use `0` when strict-priority throughput is
more important than starvation protection.
