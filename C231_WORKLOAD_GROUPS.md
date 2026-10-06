# C231 Workload Groups

`NamespaceQueryGovernor` now supports an opt-in memory reservation budget in
addition to its existing namespace concurrency and queue limits. This borrows
the useful part of ClickHouse-style workload groups without adding a global
scheduler or changing the default execution path.

## Configuration

```go
governor, err := hatSql.NewNamespaceQueryGovernor(
    hatSql.NamespaceResourceLimits{
        MaxConcurrentQueries: 8,
        MaxQueuedQueries:     32,
        MaxMemoryBytes:       256 << 20,
    },
    nil,
)
if err != nil {
    return err
}
defer governor.Close()

result, err := governor.Execute(
    ctx,
    "analytics",
    "SELECT customer, SUM(amount) FROM CACHE('orders') GROUP BY customer",
    resolver,
    nil,
    hatSql.QueryOptions{
        MemoryReservationBytes: 32 << 20,
        MaxGroupBytes:           24 << 20,
    },
)
```

`MaxMemoryBytes: 0` is the default and disables memory admission. Existing
callers therefore retain the old behavior. `MaxNamespaceMemoryBytes` is the
configuration ceiling (`1 << 50`). Namespace overrides only tighten defaults.

## Admission Rules

- `MemoryReservationBytes` is the explicit per-query reservation.
- When it is zero and a memory budget is enabled, the governor derives a
  conservative reservation from the largest positive `MaxJoinBytes`,
  `MaxResultBytes`, `MaxSortBytes`, `MaxGroupBytes`, `MaxGroupMergeBytes`, or
  `MaxSetBytes` value.
- If no in-memory operator budget is provided, the query reserves the whole
  namespace budget. This is conservative and prevents an unbounded query from
  silently overcommitting the group.
- A reservation larger than `MaxMemoryBytes` returns
  `ErrNamespaceQueryMemoryBudgetExceeded` before the resolver runs.
- Reservations are released on success, failure, and context cancellation.
  Waiters are FIFO and observe their request context while waiting.
- `MaxQueuedQueries` bounds the memory wait queue as well as the existing
  concurrency wait queue. These are separate queues, so configure the value
  with both admission mechanisms in mind.

This is a deterministic reservation queue, not a Go heap sampler. It does not
replace operator-level limits or protect memory allocated by an arbitrary
resolver outside the SQL executor. Use `MaxJoinBytes`, `MaxGroupBytes`,
`MaxSortBytes`, `MaxSetBytes`, and related limits for the actual operator caps.

## Measurement

On the benchmark host, three runs of the end-to-end namespace governor path
measured:

| Path | Time | Memory | Allocs |
| --- | ---: | ---: | ---: |
| Default governor, memory admission off | 1.440-1.480 us/op | 3,976 B/op | 18/op |
| Memory budget enabled, reservation fits | 1.441-1.457 us/op | 3,976 B/op | 18/op |
| Reservation acquire/release primitive | 8.252-8.309 ns/op | 0 B/op | 0/op |

The enabled path was within benchmark noise for this workload and introduced
no measured allocation increase. The cost is intentional admission latency
under contention and conservative serialization of unbounded queries, not a
throughput optimization.
