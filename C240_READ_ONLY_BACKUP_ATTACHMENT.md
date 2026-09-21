# C240 Read-Only Backup Attachment

`OpenBackupReadOnlyAttachment` opens a backup for SQL inspection without
restoring it into a caller-owned data directory or opening the live database
path for writes.

Supported inputs are:

- snapshot backup bundles;
- Pebble checkpoint backup bundles; and
- Pebble incremental backup repositories. `BackupID` selects a repository
  manifest; an empty value uses `latest`.

The attachment verifies the manifest and every extracted payload checksum,
loads the snapshot or Pebble store into a detached trie, and exposes only the
`SQLSourceResolver` contract. The detached trie and staging directory are
private. `Close` is required and is idempotent; it closes the read-only Pebble
handle, destroys the detached trie, and removes temporary staging files.

```go
attachment, err := hatCache.OpenBackupReadOnlyAttachment("backup.tar.gz")
if err != nil {
    return err
}
defer attachment.Close()

err = hatCache.ExecuteSQLQueryRows(
    context.Background(),
    "FROM CACHE('jobs') AS job SELECT job.id, job.state",
    attachment.Resolver(),
    nil,
    hatCache.SQLQueryOptions{},
    func(columns []string, row hatCache.SQLRow) error {
        // inspect row
        return nil
    },
)
```

For an incremental repository:

```go
attachment, err := hatCache.OpenBackupReadOnlyAttachmentWithOptions(
    repositoryPath,
    hatCache.BackupReadOnlyAttachmentOptions{BackupID: backupID},
)
```

The resolver intentionally exposes the base read-only SQL source interface;
it does not expose trie mutation methods. Optional storage-specific resolver
extensions are not attached, so queries use the normal SQL fallback path when
an accelerator is unavailable. The tradeoff is simpler isolation and a
stable public boundary at the cost of not reusing every live-trie index or
columnar fast path.

## Measurement

Command: `make benchmark-c240-backup-attachment`. Five samples use five
operations per sample on Linux/amd64 with an AMD Ryzen 9 5950X. Both paths
read the same two-row JSON source and execute the same SQL projection. The
baseline restores a bundle into a temporary data directory, loads it, and
queries it. The attachment extracts into private staging, loads once, queries,
and closes.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | --- | ---: | ---: | ---: | --- |
| Existing restore + load + query | `2395097; 2705013; 2486089; 2518624; 2899084` | `2518624` | `294662` | `409` | baseline |
| Read-only attachment + query | `667154; 638119; 485339; 582310; 505504` | `582310` | `158766` | `253` | `4.33x` faster; `1.86x` lower transient bytes; `1.62x` fewer allocations |

A cold pre-implementation baseline was `4029309 ns/op`, `2171160 B/op`, and
`1087 allocs/op` for one operation; the paired five-operation run above is the
more stable comparison. The attachment still retains its detached trie for the
attachment lifetime, so `B/op` is transient benchmark allocation rather than
retained heap. Always close attachments in a bounded scope.

The correctness gate covers snapshot SQL reads, checkpoint reads, incremental
repository reads, corruption rejection, live-source isolation, closed-handle
errors, and staging cleanup. `-race` and `go vet` are also run through the
C240 Make targets.
