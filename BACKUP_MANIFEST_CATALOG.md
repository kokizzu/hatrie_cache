# Backup Manifest Catalog

`hatBackup.BackupManifestCatalog` durably records incremental backup
manifests, validates the complete parent graph, and resolves a recoverable
chain after process restart:

```go
catalog, err := hatBackup.NewBackupManifestCatalog("/var/lib/hatrie/backups/catalog")
if err != nil {
    return err
}
if err := catalog.Append(manifest); err != nil {
    return err
}
plan, err := catalog.Plan(latestBackupID)
```

Appends use a versioned, per-record SHA-256-framed newline-delimited log and
sync the record before returning. A valid-but-edited record is rejected unless
its frame is also updated. `Replace` writes a complete v2 log to a `0600`
sibling staging file, syncs it, atomically renames it, and syncs the parent
directory. The already-pushed v1 raw log and legacy versioned JSON-array
catalog are accepted and converted to v2 on their next append.

The catalog rejects duplicate IDs, missing parents, cycles, malformed
manifests, incomplete final records, non-regular paths, and symlink catalog
paths. The catalog instance serializes its own writers; callers sharing one
path across processes must provide process-level ownership.

## Measured Cost

Linux amd64, AMD Ryzen 9 5950X, `go test ./hat/hatBackup -run '^$' -bench
'BenchmarkBackupManifestCatalog' -benchmem -count=5`:

| Operation | Previous JSON-array rewrite | Append-log result |
| --- | ---: | ---: |
| Append, growing catalog | 5.76-10.14 ms/op | 0.70-0.74 ms/op |
| Append memory | 1.21-2.02 MB/op | 3.6-3.9 KB/op |
| Append allocations | 2,445-3,949 allocs/op | 20 allocs/op |
| Load, 32 manifests | Not measured | 260-279 us/op, 149,056 B/op, 870 allocs/op |

The v2 append path is approximately 8-14x faster and 300-500x lower in
transient memory than the rejected full-array rewrite in this workload. Loads
still parse, checksum, and validate the complete catalog; that cost is paid at
restart or explicit `Load`, not on repeated appends in one catalog instance.
