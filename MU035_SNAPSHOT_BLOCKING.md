# M-U35 Snapshot Blocking

`hatSql.SQLSourceFrontierTracker` now has an opt-in blocking query for source
snapshot readiness:

```go
tracker, err := hatSql.NewSQLSourceFrontierTracker([]hatSql.SQLSourceFrontierPartition{
	{Source: "orders", Partition: "0"},
	{Source: "orders", Partition: "1"},
})
if err != nil {
	return err
}

// Source readers call Observe or ObserveBatch as snapshot/frontier progress is
// committed. A dependent query can wait without polling.
if err := tracker.WaitUntil(ctx, 100); err != nil {
	return err
}
frontier, ready := tracker.CommonFrontier()
_ = frontier
_ = ready
```

## Contract

- `WaitUntil(ctx, target)` returns only after every configured partition has
  been observed and the minimum observed frontier is at least `target`.
- A target of zero still requires the first observation from every partition;
  an observed zero is distinct from an uninitialized partition.
- A canceled or deadline-exceeded context returns its context error.
- `Observe` and `ObserveBatch` remain monotone and idempotent. Stale updates do
  not wake waiters; an advancing update wakes all current waiters.
- Waiting is notification-based. The tracker does not start a worker and does
  not poll. It allocates at most one notification channel while a wait cycle
  is pending.
- The API is opt-in and does not change ordinary SQL execution or source
  ingestion until a caller creates a tracker and waits on it.

## Bounds And Ownership

The tracker already requires a fixed, non-empty partition set, normalizes
identifiers, and keeps an indexed minimum frontier. The new wait operation adds
no history, queue, or unbounded goroutine count. The caller owns the source
reader, checkpoint transaction, and the decision about which queries must wait.

## Benchmark

Five benchmark runs used `go test ./hat/hatSql -run '^$'
-bench '^BenchmarkM35SQLSourceFrontier' -benchmem -count=5` on an AMD Ryzen 9
5950X. Values below are medians of the five reported runs.

| Path | Median ns/op | B/op | allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Existing `ReadyAt` comparator | 5.145 | 0 | 0 | Baseline query |
| `WaitUntil` before ready fast path | 7.586 | 0 | 0 | Initial implementation |
| `WaitUntil` after ready fast path | 6.363 | 0 | 0 | 1.19x faster than initial implementation |
| Existing `ReadyAt` after change | 5.035 | 0 | 0 | Within benchmark noise of baseline |

The ready wait remains about 1.26x the direct readiness query because it adds a
blocking-loop branch, but it has no per-call allocation. The tracker gains one
nil channel field; a channel is allocated only while a waiter is actually
blocked. This is a small fixed control-plane cost for a caller-selected
correctness boundary, with no default query-path overhead.

## Verification

Focused tests cover multi-partition blocking, minimum-frontier semantics,
observed-zero handling, cancellation, nil receivers, and zero allocations on
the ready path. The focused package test, race test, vet test, and broader
package verification are run by the feature Makefile targets.
