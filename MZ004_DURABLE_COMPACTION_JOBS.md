# MZ-004 Durable Compaction Jobs

Status: partially adopted. The existing `FrontierCompactionScheduler` remains
opt-in and controls frontier-safe admission. This addition supplies an
importable, bounded job catalog that callers can snapshot before scheduling and
restore after a process restart.

## Usage

```go
ledger, err := hatPipeline.NewFrontierCompactionJobLedger(
	hatPipeline.FrontierCompactionJobLedgerOptions{MaxJobs: 1024},
)
if err != nil {
	return err
}

job, err := ledger.Enqueue("orders-eu", 1200)
if err != nil {
	return err
}
if err := ledger.SaveDurableSnapshot(ctx, store); err != nil {
	return err
}
if err := ledger.Claim(job.ID); err != nil {
	return err
}
if err := ledger.SaveDurableSnapshot(ctx, store); err != nil {
	return err
}

err = scheduler.Submit(ctx, job.FrontierID, job.Boundary, func(taskCtx context.Context) error {
	taskErr := compact(taskCtx, job.Boundary)
	if taskErr == nil {
		taskErr = ledger.Complete(job.ID)
	} else {
		_ = ledger.Fail(job.ID)
	}
	if saveErr := ledger.SaveDurableSnapshot(taskCtx, store); taskErr == nil {
		taskErr = saveErr
	}
	return taskErr
})
```

`FrontierSnapshotStore` is reused for persistence. The existing
`FrontierSnapshotFileStore` uses a private temporary file, `fsync`, atomic
rename, and directory sync, so callers can choose a different durable store
without coupling the pipeline package to a filesystem or database.

## Recovery Contract

`RestoreDurableSnapshot` returns `found=false` when no snapshot exists. A
restored `running` job becomes `pending`: the process may have crashed before
the compaction task completed, so recovery must resubmit it. `pending` remains
pending; `completed` and `failed` remain available for inspection until
`PruneCompletedThrough` removes them. The catalog refuses to restore over
existing jobs and enforces a maximum job count.

The ledger does not make an external compaction task exactly once. A crash
after the task changes storage but before the completed snapshot is durable can
cause a retry. Compaction callbacks must therefore be idempotent or use their
own task-generation fence. Automatic scheduler recovery and priority selection
remain caller policy, not hidden background behavior.

## Snapshot Format And Bounds

The `HCJ1` binary snapshot contains the next job ID, bounded job records, and a
CRC32C checksum. Frontier IDs are limited to 256 bytes, the default catalog is
limited to 1,024 jobs, and snapshots are limited to 64 MiB. The format is
deterministic and stores no pointers, channels, or timestamps.

## Measurement

The same 128-job fixture was encoded and decoded five times with
`make benchmark-mz004-durable-jobs` on Linux/amd64, AMD Ryzen 9 5950X.

| Operation | JSON baseline | HCJ1 binary | Improvement |
| --- | ---: | ---: | ---: |
| Encode | 15,575 ns/op, 8,254 B/op, 2 allocs | 2,613 ns/op, 4,864 B/op, 1 alloc | 5.96x faster, 1.70x lower heap |
| Decode | 106,570 ns/op, 14,328 B/op, 144 allocs | 4,965 ns/op, 7,424 B/op, 129 allocs | 21.46x faster, 1.93x lower heap |
| Snapshot wire size | 7,992 bytes | 1,806 bytes | 4.43x smaller |

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#mz-004-durable-compaction-jobs).
The feature is opt-in, so the ordinary scheduler and existing in-memory
frontier paths pay no job-ledger or snapshot cost.

## Verification

```text
make format-mz004-durable-jobs
make test-mz004-durable-jobs
make benchmark-mz004-durable-jobs
make race-mz004-durable-jobs
make vet-mz004-durable-jobs
make test-mz004-package
```
