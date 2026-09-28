# C230 Memory-Overcommit Admission

Concurrent SQL queries can each pass an individual operator limit while their
combined working sets exhaust process memory. C230 adds an opt-in shared
reservation queue so a query waits for capacity before execution instead of
starting and later being canceled by memory pressure.

## Usage

```go
admission, err := hatSql.NewSQLMemoryAdmission(hatSql.SQLMemoryAdmissionOptions{
	MaxBytes:   512 << 20,
	MaxPending: 64,
})
if err != nil {
	return err
}
defer admission.Close()

result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	MemoryAdmission:         admission,
	MemoryReservationBytes: 64 << 20,
})
```

`MemoryReservationBytes` is a conservative caller-declared peak working-set
estimate. A positive reservation requires `MemoryAdmission`; zero leaves the
existing query path unchanged. The controller does not pretend to measure the
Go heap and does not replace `SQLOperatorMemoryTracker`: the tracker still
enforces its per-operator limit once a query is running.

## Semantics

- Reservations that fit are admitted immediately.
- When the budget is full, requests wait FIFO until earlier reservations
  release their bytes.
- A canceled context removes its waiter and returns `context.Canceled`
  without consuming capacity.
- A request larger than `MaxBytes` is rejected immediately because it can
  never fit.
- `MaxPending` bounds retained waiter state; a full queue returns
  `ErrSQLMemoryAdmissionQueueFull`.
- `Close` rejects queued and future requests. Existing leases remain valid and
  release normally.
- The returned release function is idempotent, so deferred cleanup is safe.

The controller is safe for concurrent callers. `Stats()` exposes active bytes,
pending count, admission/wait/rejection/cancellation counters, and configured
limits for monitoring. The default is fully disabled: no controller is
created, and a zero reservation adds no query memory accounting.

FIFO ordering intentionally avoids letting many small requests bypass one
large request. Operators that prefer a separate small-query pool can create a
second controller with its own budget rather than changing fairness globally.
