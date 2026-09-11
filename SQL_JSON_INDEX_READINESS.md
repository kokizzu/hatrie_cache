# SQL JSON Index Readiness

`HatTrie.WaitSQLJSONIndexReady` provides an explicit readiness barrier for a
configured SQL JSON index. This adopts the useful operational boundary behind
Materialize's collection frontiers: a startup, deployment, or failover check
can wait for a known current index instead of guessing from elapsed time or
polling maintenance counters.

## API

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

stats, available, err := trie.WaitSQLJSONIndexReady(ctx, "orders", "state")
if err != nil {
    return err
}
if !available {
    return fmt.Errorf("orders.state index is not configured")
}
log.Printf("ready index rows=%d rebuilds=%d", stats.Rows, stats.Rebuilds)
```

The method behaves as follows:

- A current configured index returns immediately with its existing
  `SQLJSONIndexMaintenanceStats`.
- An unbuilt or stale configured index is coalesced into the existing rebuild
  queue, then one cooperative rebuild is run under `ctx`. The method repeats
  until that field is current.
- An unconfigured key or field returns `available=false` without changing the
  cache.
- A nil context is rejected. Cancellation is checked before scheduling and by
  the existing progress-aware rebuild runner between rebuild units.
- Source decode or rebuild errors are returned to the caller, while the
  existing queue keeps a failed request pending for a later retry.

The barrier owns no background goroutine and adds no persistent per-index
waiter state. Callers that already run `StartSQLJSONIndexRebuildWorker` can
continue doing so; this method is intended for deterministic readiness checks
that should complete the requested rebuild themselves. `SQLJSONIndexHealth`
and `SQLJSONIndexMaintenanceStats` retain their existing inspection semantics.

The method does not change query results, lazy-refresh behavior, persistence or
wire formats, backup/restore formats, or index storage layout. It is a
readiness and operational correctness API, not a query-speed optimization.

## Example With A Deployment Check

```go
if err := trie.CreateSQLJSONFieldIndex("orders", "state"); err != nil {
    return err
}
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if _, available, err := trie.WaitSQLJSONIndexReady(ctx, "orders", "state"); err != nil {
    return fmt.Errorf("hydrate orders.state index: %w", err)
} else if !available {
    return fmt.Errorf("orders.state index disappeared during readiness check")
}
```

After a source update, calling the barrier again waits for the new source
generation. This makes a deployment check precise without exposing indexed row
values or requiring callers to use a sleep-and-poll loop.

## Benchmark And Tradeoff

The benchmark uses one already-current one-row field index and reports five
samples on an AMD Ryzen 9 5950X, `linux/amd64`. The pre-change row is the
existing maintenance-status call measured before the barrier was added. The
post-change control is the same status call in the same run; the small change
between runs is normal benchmark noise. The important comparison is the
barrier against its same-run control.

| Workload | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Existing status, before | 149.8 | 0 | 0 | `1.00x` |
| Existing status, after | 163.8 | 0 | 0 | `1.00x` control |
| Ready barrier, current index | 173.7 | 0 | 0 | `1.06x` time, same memory |

The current-index barrier costs about 10 ns over the status check and remains
allocation-free. The first call on an unready or stale index is dominated by
JSON decode and index rebuild work, which is intentionally not hidden behind a
small readiness-loop number. Run the reproducible benchmark with:

```sh
make benchmark-mz022-index-readiness
```

Raw output is written to
`build/benchmarks/mz022-index-readiness.txt`.
