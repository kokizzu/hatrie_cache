# T213: Scheduled Snapshot Checkpoints

## Purpose

Scheduled snapshots provide an opt-in checkpoint worker for deployments that
need a regularly restorable snapshot and a journal sequence to retain or ship.
The default behavior is unchanged: no goroutine starts and no snapshot is
written unless `StartScheduledSnapshots` is called.

## Configuration

```go
scheduler, err := hatCache.StartScheduledSnapshots(
	context.Background(),
	journal,
	trie,
	hatCache.ScheduledSnapshotOptions{
		Interval:       5 * time.Minute,
		SnapshotPath:   "/var/lib/hatrie/checkpoint.snapshot",
		ManifestPath:   "/var/lib/hatrie/checkpoint.manifest.json", // optional
		Format:         hatCache.SnapshotFormatBinary,
		RunImmediately: true,
	},
)
if err != nil {
	return err
}
defer scheduler.Close()
```

`Interval` and `SnapshotPath` are required. `ManifestPath` defaults to
`SnapshotPath + ".manifest.json"`; the paths must differ. An empty `Format`
uses the existing default snapshot format. `RunImmediately` controls whether
the first checkpoint is attempted when the worker starts. The scheduler is
safe to combine with explicit `RunOnce` calls; runs are serialized.

## Publication And Restore

Each run does the following while holding the journal snapshot lock:

1. Stream the snapshot into a private temporary file.
2. Flush and sync the snapshot file.
3. Write a manifest containing the snapshot format, byte count, SHA-256, and
   journal sequence; flush and sync that file.
4. Atomically rename the snapshot and then the manifest, and sync their
   parent directories.
5. Compact the journal only after the files are published.

The manifest path is the publication marker. Readers must verify it before
loading the snapshot:

```go
manifest, err := hatCache.VerifyScheduledSnapshotManifest(manifestPath)
if err != nil {
	// Retry later or keep the previous known-good checkpoint.
	return err
}
_ = manifest
if err := trie.LoadSnapshot(snapshotPath); err != nil {
	return err
}
```

The two filesystem renames cannot be one atomic operation on ordinary filesystems.
If a process stops between them, verification rejects the mismatched pair; a
reader must retry or retain the previous verified checkpoint. A successful
manifest verification proves that the bytes being loaded match the checkpoint
metadata. Failed compaction leaves the published checkpoint usable and does
not discard WAL safety.

`ReadScheduledSnapshotManifest` parses and validates only the manifest
schema. `VerifyScheduledSnapshotManifest` additionally resolves its relative
snapshot path and checks the snapshot checksum and size. Treat manifest files
as local operator-controlled input; do not accept arbitrary untrusted paths
without an application-level allowlist.

## Lifecycle And Status

`RunOnce` returns the publication result and records success or failure in
`Status`. `Status.Completed`, `Status.Failed`, `Status.LastResult`, and
`Status.LastError` are intended for health reporting. `Close` cancels the
worker, waits for any in-flight run, and is idempotent. A canceled context or
closed journal prevents compaction and is reported as an error.

## Cost

This is a durability/operability feature, not a speed optimization. The
paired benchmark uses the existing binary snapshot path and three repetitions
on an AMD Ryzen 9 5950X:

| Mode | Median time | Bytes/op | Allocs/op | Relative time | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing snapshot | 3.750 ms | 129,403 | 59 | 1.00x | 1.00x |
| Scheduled snapshot + manifest | 4.472 ms | 135,607 | 81 | 1.19x | 1.05x |

The scheduled path adds 22 allocations and about 6.2 KB per checkpoint for
the manifest and paired publication. Filesystem sync latency is storage
dependent; these numbers are a local reference, not an SLA.

## Verification

Focused correctness and race checks:

```text
make test-t213
make test-t213-package
make race-t213
make vet-t213
make benchmark-t213
make cleanup-hatrie-tmp-after-test
make audit-hatrie-tmp
```

The cleanup target removes stale test metadata and old temporary build/test
artifacts while preserving active worktrees.
