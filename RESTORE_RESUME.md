# Restore Resume Checkpoints

Restore resume is an opt-in recovery aid for large atomic bundles and
content-addressed Pebble backup repositories. The normal restore path is
unchanged and deletes its random staging directory on failure.

## Enable It

Use the CLI flag when restoring locally:

```sh
make restore-bundle RESTORE_BUNDLE_PATH=backup/run-001.tar.gz DATA_DIR=data RESTORE_BUNDLE_RESUME=true
```

The importable API exposes the same switch:

```go
report, err := hatCache.RestoreBackupBundle(
    "backup/run-001.tar.gz",
    "data",
    hatCache.BackupBundleRestoreOptions{Resume: true},
)
```

The `Resume` option defaults to `false`. It applies to snapshot bundles,
Pebble checkpoint bundles, and incremental repository restores. It does not
change the backup format or the published data directory.

## How Retry Works

With `Resume: true`, the restore creates a private sibling directory named
`.DATA_DIR.restore-resume`. A failed restore leaves that directory in place.
The next restore validates the archive or repository manifest again, removes
stale files from the checkpoint, and streams every declared payload. A payload
is reused only when its existing size and SHA-256 match the current declaration;
otherwise it is rewritten through a temporary file and atomically renamed.

The archive is still read from beginning to end because tar payloads are
sequential and every byte must be checked. Resume primarily avoids rewriting
payloads completed before an interruption. Once semantic verification,
filesystem synchronization, and atomic publication succeed, the checkpoint
directory is renamed into `DATA_DIR` and no resume directory remains.

Do not manually edit or share the checkpoint directory. Symlinks, special
files, and symlinked parents are rejected. Keep the source bundle/repository
stable until the retry succeeds. A failed retry intentionally leaves the
checkpoint for diagnosis or another retry; remove it only after deciding to
start over.

## Recovery Procedure

1. Confirm that the source bundle or repository is readable and unchanged.
2. Confirm that no other restore process is targeting the same `DATA_DIR`.
3. Repeat the restore with `RESTORE_BUNDLE_RESUME=true` or `Resume: true`.
4. Run `make doctor DOCTOR_PATH=data` and, for a production rehearsal, run
   `make restore-rehearsal RESTORE_REHEARSAL_PATH=backup/run-001.tar.gz`.
5. If the checkpoint is intentionally abandoned, stop restore attempts and
   remove the sibling `.DATA_DIR.restore-resume` directory using the same
   controlled host cleanup procedure used for other restore staging data.

## Cost And Limits

Resume adds a staging walk and a SHA-256 read of existing candidate files, so
it is not a universal throughput optimization. It trades those checks for
avoided file writes after interruption. The focused 2,048-key binary snapshot
benchmark measured fresh extraction at a median 853,533 ns/op, 82,551 B/op,
and 82 allocations; repeated checkpoint extraction measured 834,838 ns/op,
118,491 B/op, and 112 allocations. Archive bytes read are unchanged. Results
are host and filesystem dependent; use resume for recoverability, not as a
general speed setting.
