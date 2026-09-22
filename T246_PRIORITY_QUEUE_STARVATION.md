# T246: Priority Queue Starvation Bounds

`hatDataStructure.PriorityVisibilityQueue` now supports an opt-in starvation
bound for priority work:

```go
queue := hatDataStructure.NewPriorityVisibilityQueueWithOptions[string](
	hatDataStructure.PriorityVisibilityQueueOptions{
		VisibilityTimeout: time.Minute,
		StarvationAfter:   64,
	},
)
```

`StarvationAfter` is the maximum number of leases that may bypass a ready item
before that item is selected, regardless of its numeric priority. Equal
priorities remain FIFO. `0` is the default and preserves strict priority
ordering with no fairness scan.

The policy is included in in-memory snapshots and binary snapshots. Binary
snapshots written by this version use format v2; v1 snapshots remain readable
and restore with fairness disabled. Snapshot values include the pending item
wait counters, so restoring a queue does not reset an item’s starvation bound.

## Tradeoff

Fairness requires scanning the ready heap when enabled. It is intended for
mixed-priority workloads where an urgent stream must not permanently starve
background work, not for the lowest-latency strict-priority path.

On the benchmark host (AMD Ryzen 9 5950X, Linux amd64), all cases remained at
`0 B/op` and `0 allocs/op`:

| Workload | Median ns/op | Relative |
| --- | ---: | ---: |
| Existing strict-priority path before T246 | 125.0 | 1.00x |
| Strict-priority path after T246, controlled baseline | 136.9 | 1.10x vs separate pre-change run |
| T246 strict-priority stream | 135.3 | 0.99x vs controlled baseline |
| T246 bounded-after-64 stream | 133.8 | 0.98x vs controlled baseline |
| Resident 256-item strict-priority queue | 179.6 | 1.00x |
| Resident 256-item bounded-after-64 queue | 876.7 | 4.88x |

The separate pre-change run varied from 119.7 to 130.8 ns/op, so the
controlled same-source baseline is the meaningful default-path comparison.
The resident fairness cost is deliberate and opt-in; users who need strict
priority should leave `StarvationAfter` at zero.

Focused verification:

```text
make test-t246-priority-queue
make test-t246-priority-queue-package
make race-t246-priority-queue
make vet-t246-priority-queue
make benchmark-t246-priority-queue
```
