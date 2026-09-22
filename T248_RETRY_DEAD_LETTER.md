# T248: Retry Counters And Dead-Letter Routing

T248 adds an opt-in retry policy around
`DeduplicatingPriorityVisibilityQueue[T]`:
`RetryingDeduplicatingPriorityVisibilityQueue[T]` counts delivery attempts and
routs terminal failures to a FIFO dead-letter collection.

## Semantics

- `MaxAttempts == 0` means unlimited retries.
- Every successful lease increments `Attempts`; `MaxAttempts: 2` permits two
  deliveries.
- `Nack` and `NackToken` route a task when the current attempt reaches the
  limit instead of putting it back in active work.
- `RequeueExpired` applies the same policy to worker crashes or lost leases.
- Dead letters expose the original key, priority, value, attempt count, and a
  `RetryDeadLetterReason` of `max-attempts` or `visibility-expired`.
- `PopDeadLetter` removes the oldest record. `DeadLetters(dst)` copies all
  waiting records into caller-owned storage.
- Routing acknowledges the active task and releases its deduplication key, so
  an operator can enqueue a corrected replacement after inspecting the dead
  letter.
- The base priority queue and T247 queue are unchanged when this wrapper is
  not used.

```go
queue := hatDataStructure.NewRetryingDeduplicatingPriorityVisibilityQueue[
    string,
](10000, time.Minute, 3)

queue.Enqueue("invoice-42", 10, "send invoice")
item, ok := queue.Lease(time.Now())
if ok && !queue.Nack(item.ID, time.Time{}) {
    // At attempt three, Nack routes the item to the dead-letter collection.
}

dead, ok := queue.PopDeadLetter()
if ok {
    log.Printf("failed %s after %d attempts: %s", dead.Key, dead.Attempts, dead.Reason)
}
```

## Checkpoint And Recovery

`Snapshot` and `RestoreRetryingDeduplicatingPriorityVisibilityQueue` preserve
active pending/leased work, the retry limit, and waiting dead letters.
`MarshalSnapshot` and `UnmarshalRetryingDeduplicatingPriorityVisibilityQueue`
use a bounded CRC-protected binary envelope. `Save...` publishes atomically
with mode `0600`; `Load...` advances the epoch so tokens from before restart
are rejected. The value codec is supplied by the application, just as for the
T247 queue.

## Measurements

Command: `make benchmark-t248-retrying-queue` (five samples,
`-benchmem`, Linux/amd64, AMD Ryzen 9 5950X). The retry cycle performs enqueue,
lease, nack, lease, and ack. The terminal cycle performs enqueue, lease, nack
to dead letter, and pop; it is not the same number of operations as the retry
cycle.

```text
Base retry cycle:       426.1 480.5 452.7 474.7 461.7 ns/op, 0 B/op, 0 allocs/op
Retrying retry cycle:   535.8 553.0 568.9 536.2 617.5 ns/op, 0 B/op, 0 allocs/op
Dead-letter cycle:      320.9 320.0 295.5 310.0 315.1 ns/op, 0 B/op, 0 allocs/op
Base active-256 cycle:  344.0 322.4 339.3 302.7 297.9 ns/op, 0 B/op, 0 allocs/op
Retry active-256 cycle: 364.0 354.0 373.2 390.2 349.5 ns/op, 0 B/op, 0 allocs/op
```

| Workload | Base median | Retrying median | Relative result |
| --- | ---: | ---: | --- |
| Retry cycle | 461.7 ns/op | 553.0 ns/op | 1.20x CPU, allocation-neutral |
| 256 active leases | 322.4 ns/op | 364.0 ns/op | 1.13x CPU, allocation-neutral |
| Terminal route + pop | not directly comparable | 315.1 ns/op | 0 B/op, 0 allocs/op after buffer reuse |

`make memory-t248-retrying-queue` measured 10,000 simultaneously leased items
at approximately 4.735 MB for the T247 base and 6.188 MB for T248, or 473.5
versus 618.8 bytes per active item and 1.31x retained heap. The extra reverse
lease map makes expiry routing constant-time between expiry events. The
earliest-expiry guard prevents an O(n) scan on every lease; the scan runs only
when an expiry is due. Small dead-letter buffers are reused, while buffers
larger than 64 records are released after draining to avoid retaining a large
peak.
