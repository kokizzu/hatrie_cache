# Visibility Queue

`hatDataStructure.VisibilityQueue[T]` is a generic, importable queue for work
that must be hidden from other consumers while a worker processes it. It adopts
the visibility-timeout and acknowledgement model used by Tarantool-style queue
workers without adding a background goroutine or changing the existing
`DelayQueue` API.

## Semantics

- The type is non-thread-safe, like the other queues in `hatDataStructure`.
- `Enqueue` makes a value immediately available. `EnqueueAt` and `EnqueueAfter`
  support delayed availability.
- `Lease` returns one ready value and hides it until `LeaseUntil`.
- `Ack` permanently completes an active lease.
- `Nack` returns an active lease at a caller-selected time while retaining its
  ID and attempt count.
- `RequeueExpired` returns expired leases immediately. `Lease` calls it first,
  so a caller that polls with `Lease` does not need a separate timer loop.
- `Attempts` starts at one and is retained across nacks and timeout recovery.
- A positive capacity counts both pending and leased values. Zero capacity is
  unbounded.
- The zero value is usable and uses a one-minute visibility timeout.
- `Clear` drops all state but keeps lease IDs monotonic, so a stale handle cannot
  acknowledge a later lease after the queue is reused.

The expiry index removes acknowledged or negatively acknowledged deadlines in
O(log n). This avoids retaining stale timeout entries until their old deadline.
Steady-state queue operations do not allocate after the queue has grown to its
working size.

## Example

```go
now := time.Unix(100, 0).UTC()
queue := hatDataStructure.NewVisibilityQueue[string](1024, 30*time.Second)

queue.Enqueue("job-42")
item, ok := queue.Lease(now)
// item.ID == 1, item.Value == "job-42", item.Attempts == 1
// item.LeaseUntil == now.Add(30 * time.Second)

if ok {
    // On success:
    queue.Ack(item.ID)
}
```

For a retry, return the item to the queue instead:

```go
queue.Nack(item.ID, now.Add(2*time.Second))
retry, ok := queue.Lease(now.Add(2 * time.Second))
// retry.ID == item.ID and retry.Attempts == 2
if ok {
    queue.Ack(retry.ID)
}
```

`RequeueExpired(now)` returns the number of leases recovered at `now`. A
process crash before `Ack` therefore does not lose a leased item permanently,
provided another worker eventually polls the queue or calls
`RequeueExpired`. The queue itself is in-memory; pair it with the command
journal or replication outbox when durable recovery is required. A caller can
send items that exceed its retry policy to the existing `DeadLetterQueue`.

## Benchmark

Run:

```text
make benchmark-visibility-queue
```

The benchmark reports zero bytes and zero allocations per steady-state
operation. The 256-active-lease case is slower than the one-active case because
it exercises indexed expiry removal among 256 live leases. See the raw samples
and the comparison caveat in [BENCHMARK.md](BENCHMARK.md#visibility-timeout-queue).
# Epoch-fenced leases

For leases that can cross a process or storage boundary, construct the queue
with `NewVisibilityQueueWithEpoch` and use `LeaseWithToken`, `AckToken`, and
`NackToken`. The token contains both the queue epoch and the monotonic lease
ID, so a token from an older process incarnation cannot acknowledge or nack a
new lease that reused the same numeric ID.

Epoch `0` uses `DefaultVisibilityQueueEpoch` (`1`). Persist the epoch with the
queue ownership metadata and advance it before restoring the queue in a new
process. The legacy `Lease`, `Ack(id)`, and `Nack(id, ...)` methods remain
available and retain their original behavior; use the token API for restart
fencing.

## Benchmark

Linux, AMD Ryzen 9 5950X, Go benchmarks with `-benchmem`; each value is the
median of three runs:

| Workload | ns/op | B/op | allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Legacy lease + raw-ID ack, one active item | 107.0 | 0 | 0 | Baseline |
| Epoch token lease + token ack, one active item | 115.2 | 0 | 0 | 1.08x CPU |
| Legacy lease + raw-ID ack, 256 resident items | 545.1 | 0 | 0 | Baseline |
| Epoch token lease + token ack, 256 resident items | 593.4 | 0 | 0 | 1.09x CPU |

The added epoch is one `uint64` per queue. The legacy hot path keeps the same
item layout and remains allocation-free; the token path is also allocation-free
but pays a small explicit validation cost for restart safety.
