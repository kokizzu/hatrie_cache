# T-G21 Hot Backup With an Exact WAL Coordinate

T-G21 adopts the Tarantool-style hot-backup idea: stream a consistent snapshot
while mutations continue, and publish the exact journal sequence needed to
continue recovery from that snapshot.

## API

```go
result, err := hatCache.CreateHotBackupBundle(
    "backup.tar.gz",
    trie,
    journal,
    hatCache.BackupBundleOptions{
        SnapshotFormat: hatCache.SnapshotFormatBinary,
    },
)
if err != nil {
    return err
}
fmt.Println(result.Bundle.JournalSequence)
```

`CreateHotBackupBundleWithContext` is the cancellable form. The returned
`HotBackupResult` contains both the standard recoverable `BackupBundleManifest`
and the exact `SnapshotManifest` used to create it.

## Guarantees

- The snapshot stream captures one journal sequence at a short capture
  barrier, then releases the journal mutex while snapshot bytes are copied.
- The returned snapshot sequence, bundle sequence, checkpoint journal, and
  per-file checksum all describe the same recovery boundary.
- The bundle format is unchanged: existing `VerifyBackupBundle` and
  `RestoreBackupBundle` validate and restore hot bundles.
- Bundle publication remains atomic. Cancellation or a failed checksum leaves
  the previous destination bundle untouched.
- The default `CreateBackupBundle` API remains available for compatibility.

Hot backup currently supports full snapshot-mode bundles. Filtered snapshots,
Pebble checkpoint mode, and incremental repositories keep their existing APIs
because each needs a different online storage contract.

## Recovery

1. Verify the archive with `VerifyBackupBundle`.
2. Restore it with `RestoreBackupBundle`.
3. Start the restored node at the reported journal sequence and retain the
   live journal from that sequence onward for catch-up.

The bundled `commands.journal` contains a checkpoint at the snapshot boundary;
it is not a copy of mutations that happened after that boundary.

## Verification And Tradeoff

Focused correctness coverage is in
`hat/hatCache/hot_backup_test.go`. The paired benchmark compares the existing
mutex-held snapshot bundle path with the hot path:

```text
make test-hot-backup-tg21
make benchmark-hot-backup-tg21
```

Raw benchmark output is recorded in `BENCHMARK.md` after the feature package
build is green. The hot path is intended to reduce writer pause time; total
backup wall time can be similar because both paths still stream and checksum
the same snapshot and archive payload.
