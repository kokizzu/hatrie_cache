# CH-006: Durable Mutation Dependency Queue

The existing `SQLMutationDependencyGraph` coordinates dependency-aware work in
memory and supports caller-managed snapshots. `SQLMutationDependencyQueue` is
the opt-in durable variant for mutations that must survive process restart.

## Example

```go
queue, err := hatSql.OpenSQLMutationDependencyQueue("/var/lib/app/mutations.log", 4096)
if err != nil {
    return err
}
defer queue.Close()

if err := queue.Add(hatSql.SQLMutationTask{ID: "delete-old-rows"}); err != nil {
    return err
}
ready, err := queue.ClaimReady(1)
if err != nil {
    return err
}
if len(ready) == 1 {
    if err := queue.Complete(ready[0].ID, ready[0].Attempt); err != nil {
        return err
    }
}
```

Reopening the same path replays the durable log:

```go
reopened, err := hatSql.OpenSQLMutationDependencyQueue("/var/lib/app/mutations.log", 4096)
if err != nil {
    return err
}
defer reopened.Close()
task, ok := reopened.Task("delete-old-rows")
// task.State is SQLMutationTaskCompleted after replay.
```

## Durability model

- Each successful add, claim, completion, failure, retry, or requeue is one
  versioned binary WAL record.
- Records contain a monotone sequence, bounded payload, and CRC-32 checksum.
- The record is synced before the operation returns, so the queue is strictly
  durable by default.
- A partial final record is treated as an interrupted write and truncated on
  open. A complete record with a bad checksum, invalid sequence, or invalid
  graph transition fails open instead of being silently discarded.
- `Compact` rewrites the current graph to one snapshot record, keeping replay
  time and log size bounded. Compaction is caller-triggered and atomic through
  a same-directory temporary file and rename.
- The queue creates a new log with mode `0600`; callers still own directory
  permissions, disk quotas, backups, and retention.

The standalone `SQLMutationDependencyGraph` is unchanged and remains the
lower-latency choice when the caller already owns checkpointing. The durable
queue does not execute SQL itself; applications connect task IDs to their
`ALTER`, `DELETE`, index, or compaction workers.

## Measured tradeoff

The benchmark uses 512 ready tasks and claims/requeues 64 tasks per measured
operation. Medians are from five runs on an AMD Ryzen 9 5950X. The queue
benchmark uses `-benchtime=100x`; the graph values are the same run's control.

| Path | ns/op | B/op | allocs/op | Relative to graph |
| --- | ---: | ---: | ---: | ---: |
| In-memory graph claim/requeue | 53,119 | 14,848 | 2 | 1.00x |
| Durable queue claim/requeue | 1,502,467 | 21,079 | 13 | 28.29x slower, 1.42x heap, 6.50x allocations |

The durable queue's large CPU cost is the intentional per-transition `fsync`,
not a regression in the default graph. Compared with the first queue
implementation, the final path is 1.04x faster, uses 5.79x less heap, and uses
1.31x fewer allocations by removing the full graph snapshot from the success
path. Restart replay of 256 added tasks measured a median 1,073,066 ns/op,
90,925 B/op, and 1,303 allocations/op.

Use the in-memory graph for throughput-first scheduling and the durable queue
when restart safety is worth the sync cost. Automatic SQL mutation integration,
replication, and cross-process lease ownership remain future work.

See [BENCHMARK.md](BENCHMARK.md#ch-006-durable-mutation-dependency-queue) for
raw samples and the exact command.
