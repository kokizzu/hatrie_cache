# Object-Store Backups

`hatBackup.ObjectStoreTarget` adds an object-store backup target without
coupling the project to one cloud SDK. An adapter can map the two operations to
S3, GCS, Azure Blob Storage, or an internal immutable-object service:

```go
type ObjectStore interface {
	Put(ctx context.Context, key string, body io.Reader, size int64) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}
```

Create a target with a store and an optional object-key prefix:

```go
target, err := hatBackup.NewObjectStoreTarget(store, "backups/node-a")
if err != nil {
	return err
}
manifest, err := target.Backup(ctx, dataDir, hatBackup.BundleManifest{
	Mode:     hatBackup.ModeSnapshot,
	Snapshot: "snapshot-2026-09-06",
})
if err != nil {
	return err
}
_, err = target.Restore(ctx, restoreDir, false)
```

## Object layout

Each payload is stored below the prefix using its slash-separated relative file
path. The target stores the bundle manifest at `manifest.json` below the same
prefix. The manifest records every file's path, byte count, and SHA-256 digest.
The source file named `manifest.json` is reserved and rejected to avoid a data
file colliding with the bundle metadata.

Backup uploads payloads first and publishes the manifest last. A partially
completed upload therefore has no complete manifest and cannot be selected as a
finished bundle by this target. Existing objects not referenced by the newest
manifest are harmless stale objects and can be removed by a provider-specific
retention job.

## Restore guarantees

Restore downloads and validates the manifest before creating any destination
files. It rejects unsupported manifest versions, duplicate paths, absolute or
traversal paths, symlink components, invalid sizes, and malformed checksums.
Each object is streamed to a private staging directory and checked against the
manifest's exact size and SHA-256 digest. After the complete tree is synced,
the existing atomic restore publication path replaces or creates the requested
destination according to the `overwrite` flag.

The object-store adapter must consume exactly the `size` bytes passed to
`Put`, return a fresh reader from `Get`, and honor the supplied context. The
target does not buffer whole backup files in memory.

`DefaultObjectStoreManifestMaxBytes` limits manifest metadata to 8 MiB. File
payloads are not subject to that manifest limit and are streamed directly
between the local filesystem and the adapter.

This feature is additive: existing local backup and restore modes are unchanged,
and no existing on-disk or wire format is modified.
