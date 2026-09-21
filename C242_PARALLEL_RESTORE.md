# C242: Bounded Parallel Restore

This adopts a ClickHouse-style part-oriented restore optimization for Hatrie
Cache's content-addressed incremental backup repository. Independent repository
objects can be copied and checksum-verified concurrently before the existing
atomic publish step.

## Behavior

`BackupBundleRestoreOptions.MaxPartConcurrency` controls the number of
independent repository files copied at once:

```go
report, err := hatCache.RestoreBackupRepository(
	repositoryPath,
	backupID,
	dataDir,
	hatCache.BackupBundleRestoreOptions{MaxPartConcurrency: 4},
)
```

The CLI equivalent is:

```sh
make cli ARGS='restore-bundle -bundle backup/pebble-repository -data-dir data -max-part-concurrency 4'
```

The default is `0`, which means serial restore. `1` is also serial; values
above `256` are rejected. The setting affects only fresh restore of a
content-addressed incremental repository. A gzip bundle remains serial because
its payload is one compressed stream, and `-resume` keeps the existing
reuse-in-place behavior so verified staging files can be reused safely.

Each copied file is created exclusively, streamed through SHA-256 and size
verification, and removed if verification fails. The destination is still
published only after all files and the semantic backup checks pass, so the
parallel path does not change atomic restore or rollback behavior.

## Measured Tradeoff

`make benchmark-c242-parallel-restore` on AMD Ryzen 9 5950X, Linux amd64,
16 independent files of approximately 64 KiB each, local filesystem,
`-benchtime=1s`:

| Variant | Time | Memory | Allocations | Throughput |
| --- | ---: | ---: | ---: | ---: |
| Serial (`MaxConcurrency=0`) | 2,195,293 ns/op | 546,624 B/op | 325 allocs/op | 477.65 MB/s |
| Bounded parallel (`MaxConcurrency=4`) | 1,117,681 ns/op | 548,631 B/op | 344 allocs/op | 938.17 MB/s |

The measured result is `1.96x` faster and `1.96x` higher copy throughput, with
`2,007 B/op` more transient heap (`0.37%`) and `19` more allocations per restore
of this workload. More concurrency is not automatically better: it can increase
disk contention and memory pressure, so start with `2` or `4` and measure on the
target storage device.

The existing repository-level benchmark target could not compile in this
checkout because unrelated pre-existing `hatSql` definitions are missing:
`MaxDataflowTextBytes`, `TypedTableDate`, and `TypedTableTimestamp`. The
independent `hatBackup` benchmark above is the reproducible raw measurement for
this change; the restore integration retains the existing build blocker.

## Verification

Focused correctness and safety checks:

```sh
make test-c242-parallel-restore
make race-c242-parallel-restore
make vet-c242-parallel-restore
```

The tests cover successful multi-file restore, invalid concurrency bounds, size
and checksum failure, partial-destination cleanup, and CLI validation.
