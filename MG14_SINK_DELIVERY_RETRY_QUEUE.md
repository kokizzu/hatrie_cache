# M-G14 Sink Delivery Retry Queue

`hatPipeline.SinkRetryQueue[T]` is an opt-in in-memory delivery coordinator
for pipelines with more than one sink. It gives every sink its own bounded
pending and leased queue while sharing one bounded poison-message list.

The queue composes the existing `hatDataStructure.VisibilityQueue` behavior:

- `Enqueue` and `EnqueueAt` apply per-sink capacity backpressure.
- `Lease` hides a ready value until `Ack`, `Retry`, or visibility expiry.
- `Retry` schedules a retry or moves the value to a dead-letter list after
  `MaxAttempts`.
- `RequeueExpired` makes abandoned leases available again.
- `ReplayDeadLetter` resets the attempt budget after an operator decision.
- queue ID, epoch, sink, and attempt fields fence stale or cross-sink lease
  operations.

The queue does not perform sink I/O and does not claim to be durable. Pair it
with the existing sink progress, idempotency-token, and checkpoint APIs when a
delivery must survive process loss. Advance `SinkRetryQueueOptions.Epoch` when
restoring ownership after a restart.

## Defaults

Zero-valued options use bounded operational defaults:

| Option | Default |
| --- | ---: |
| `Capacity` per sink | 1,024 pending plus leased items |
| `VisibilityTimeout` | 1 minute |
| `MaxAttempts` | 8 leases |
| `DeadLetterLimit` across all sinks | 256 values |
| `Epoch` | 1 |

The queue is disabled unless the caller constructs it and routes delivery
through it. Existing sinks and pipeline paths are unchanged.

## Example

```go
queue, err := hatPipeline.NewSinkRetryQueue[string](hatPipeline.SinkRetryQueueOptions{
    Capacity:          1024,
    VisibilityTimeout: 30 * time.Second,
    MaxAttempts:       5,
    DeadLetterLimit:   128,
    Epoch:             restoredEpoch,
})
if err != nil {
    return err
}

queue.Enqueue("warehouse", "batch-42")
lease, ok := queue.Lease("warehouse", time.Now())
if !ok {
    return nil
}

if err := deliver(lease.Value); err != nil {
    outcome, deadLetterID, valid := queue.Retry(
        lease,
        time.Now().Add(time.Second),
        err.Error(),
    )
    if !valid {
        return errors.New("stale sink lease")
    }
    if outcome == hatPipeline.SinkRetryOutcomeDeadLettered {
        log.Printf("dead-lettered %d", deadLetterID)
    }
    return nil
}
returnBool := queue.Ack(lease)
if !returnBool {
    return errors.New("stale sink lease")
}
```

For a production delivery loop, call `RequeueExpired(now)` periodically and
persist the successful sink frontier only after the external side effect is
committed. `DeadLetters()` returns a detached snapshot; replay keeps the
failure until the per-sink capacity accepts it and starts the attempt count at
one again.

## Measurement

Five `-count=5` samples on Linux/amd64 with an AMD Ryzen 9 5950X were run by
`make benchmark-mg14`. The baseline is the existing single-sink visibility
queue without sink lookup, synchronization, or dead-letter fencing.

| Workload | Existing primitive median | `SinkRetryQueue` median | CPU cost | Baseline B/op | Queue B/op | Baseline allocs/op | Queue allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Lease + ack | 89.39 ns | 227.0 ns | 2.54x slower | 0 | 0 | 0 | 0 |
| Lease + retry | 86.72 ns | 209.2 ns | 2.41x slower | 0 | 0 | 0 | 0 |

This is not a raw throughput optimization. The cost buys bounded per-sink
isolation, synchronized access, lease fencing, attempt accounting, and poison
retention. Retained memory is bounded by the configured per-sink capacity,
dead-letter limit, and one state map entry per sink; the hot operation path
adds no heap allocations in the measured steady state.
