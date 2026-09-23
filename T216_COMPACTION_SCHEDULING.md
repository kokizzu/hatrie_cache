# T216: Vinyl-Style Compaction Scheduling

T216 adds opt-in deferred compaction for `hatDataStructure.LSMTable`. Existing
callers keep synchronous compaction because `LSMCompactionImmediate` is the
zero-value default.

## Configuration

```go
table, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{
    MemtableMaxRecords:      4096,
    MaxRunsBeforeCompaction: 8,
    Compaction: hatDataStructure.LSMCompactionPolicy{
        Mode:         hatDataStructure.LSMCompactionDeferred,
        MaxDebtRuns:  4,
        MaxDebtBytes: 64 << 20,
    },
})
```

`MaxDebtRuns` and `MaxDebtBytes` are independent OR thresholds. A positive
threshold makes compaction due when it is reached. In deferred mode a zero
`MaxDebtRuns` uses `MaxRunsBeforeCompaction`; zero `MaxDebtBytes` disables the
byte threshold. Negative values and unknown modes are rejected as invalid
options.

Deferred writes still flush full memtables into immutable runs, but `Put` and
`Delete` do not compact those runs. The caller can inspect `CompactionDue()`
and call `CompactIfNeeded()`, or register tables with the scheduler:

```go
scheduler, err := hatDataStructure.NewLSMCompactionScheduler(
    hatDataStructure.LSMCompactionSchedulerOptions{MaxCompactionsPerRun: 2},
)
if err != nil {
    return err
}
if err := scheduler.Register("orders", table); err != nil {
    return err
}
result, err := scheduler.RunOnce()
```

The scheduler is caller-owned. It starts no goroutine, uses deterministic
name tie-breaking, and selects the registered table with the largest
older-run wire-byte debt first. `Unregister` removes a table without affecting
the LSM table itself.

## Accounting

`LSMTable.Stats()` now reports:

- `MemtableBytes`: key plus value payload bytes currently in the mutable table;
  tombstones count only their key bytes.
- `MemtableTombstones`: tombstones currently in the mutable table.
- `CompactionDebtRuns`: immutable runs older than the newest run.
- `CompactionDebtBytes`: wire bytes of those older runs.
- `CompactionCount`, `CompactionInputBytes`, and `CompactionOutputBytes`:
  successful compaction counters. Failed compactions do not advance them.

This is bounded structural accounting, not a process RSS measurement. It is
useful for admission and scheduling without materializing keys or starting a
global monitor.

## Operational Tradeoff

Immediate mode bounds read amplification as writes arrive, at the cost of
put/delete latency when a compaction is triggered. Deferred mode shortens the
write path and lets an application schedule work during a controlled window,
but it temporarily retains more immutable runs, consumes more run storage,
and leaves reads with higher run fan-out until maintenance runs. Scheduling
does not make the total write-plus-compaction CPU cheaper; it changes when the
cost is paid.

Compaction folds all visible records under the table lock. A failed fold keeps
the existing runs and accounting intact. The feature is in-memory LSM policy
control; it does not add replication, persistence, or a background lifecycle.

## Verification

```text
make test-t216
make test-t216-package
make race-t216
make vet-t216
make benchmark-t216-before
make benchmark-t216
make report-t216-accounting
```
