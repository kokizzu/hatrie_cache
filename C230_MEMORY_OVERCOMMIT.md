# C230: Memory-Overcommit Wait Queue

## Decision

Adopted as an opt-in SQL execution policy. `SQLMemoryOvercommitQueue` shares a
bounded retained-memory budget across concurrent queries. When an operator's
estimated working set grows beyond currently available capacity, the query
waits instead of immediately failing its operator-memory budget. Context
cancellation removes a waiting query, and a request larger than the total
capacity is rejected immediately because it could never fit.

The default remains unchanged: `SQLQueryOptions.MemoryOvercommit == nil` means
no queue, no queue allocation, and the existing execution path. The queue is
currently connected to the existing materialized `GROUP BY`, `SORT`, and
distinct set accounting points. Estimates are retained working-byte estimates,
not process RSS measurements.

```go
queue, err := hatSql.NewSQLMemoryOvercommitQueue(hatSql.SQLMemoryOvercommitOptions{
	LimitBytes: 256 << 20,
})
if err != nil {
	return err
}
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	MemoryOvercommit: queue,
})
```

`MaxWaiters` defaults to 64 and is bounded. Every successful reservation is
released when an operator shrinks or the query finishes. The queue exposes
bounded `Snapshot` counters for used bytes, waiters, grants, and cancellations.

## Verification

```sh
make test-c230-memory-overcommit
make race-c230-memory-overcommit
make benchmark-c230-memory-overcommit
```

The focused tests cover release wake-up, context cancellation, queue capacity,
operator byte deltas, query integration, and cleanup after a rejected query.

## Benchmark

The benchmark uses the same materialized `VALUES ... GROUP BY ... ORDER BY`
query for both variants and runs five samples per case on the repository's
AMD Ryzen 9 5950X host.

| Variant | Median ns/op | B/op | allocs/op | Relative CPU | Relative heap | Relative allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default, before C230 | 11,566 | 8,648 | 55 | 1.00x | 1.00x | 1.00x |
| Existing operator tracker, before C230 | 15,410 | 9,864 | 82 | 1.33x | 1.14x | 1.49x |
| Default, matched after C230 | 11,960 | 8,648 | 55 | 1.00x | 1.00x | 1.00x |
| Memory-overcommit queue, matched after C230 | 15,494 | 9,631 | 81 | 1.30x | 1.11x | 1.47x |

The matched default path retains the same allocation and heap result as the
baseline; the small CPU difference is benchmark variance. Queue mode costs
about 30% CPU, 11% heap, and 47% allocations for this small query, similar to
the existing opt-in operator tracker. That cost is intentional and only paid
when shared memory admission is requested; the benefit is bounded forward
progress under concurrent memory pressure instead of immediate cancellation or
an unbounded build.

## Safety Boundaries

- A nil queue is fully off and does not change query behavior.
- A negative configuration or reservation returns a typed error.
- A reservation larger than `LimitBytes` returns immediately.
- The wait queue has a bounded `MaxWaiters` limit.
- Context cancellation removes blocked waiters and preserves existing usage.
- Release is clamped, so cleanup cannot make usage negative.
