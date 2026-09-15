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

Appends use a versioned newline-delimited log and sync the record before
returning. `Replace` writes a complete log to a `0600` sibling staging file,
syncs it, atomically renames it, and syncs the parent directory. A legacy
versioned JSON-array catalog is accepted and converted to the append-log form
on its next append.

The catalog rejects duplicate IDs, missing parents, cycles, malformed
manifests, incomplete final records, non-regular paths, and symlink catalog
paths. The catalog instance serializes its own writers; callers sharing one
path across processes must provide process-level ownership.

## Measured Cost

Linux amd64, AMD Ryzen 9 5950X, `go test ./hat/hatBackup -run '^$' -bench
'BenchmarkBackupManifestCatalog' -benchmem -count=5`:

| Operation | Previous JSON-array rewrite | Append-log result |
| --- | ---: | ---: |
| Append, growing catalog | 5.76-10.14 ms/op | 1.16-2.02 ms/op |
| Append memory | 1.21-2.02 MB/op | 3.5-4.0 KB/op |
| Append allocations | 2,445-3,949 allocs/op | 20 allocs/op |
| Load, 32 manifests | Not measured | 270-298 us/op, 147,008 B/op, 870 allocs/op |

The append path is approximately 3-8x faster and 300-500x lower in transient
memory in this workload. Loads still parse and validate the complete catalog;
that cost is paid at restart or explicit `Load`, not on repeated appends in one
catalog instance.
