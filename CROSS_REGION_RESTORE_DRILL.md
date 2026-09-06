# Cross-Region Restore Drill

Use an immutable object-store prefix per region and backup generation. The
provider is responsible for copying the objects between regions; `hatBackup`
validates the copied prefix before it is restored.

```go
source, _ := hatBackup.NewObjectStoreTarget(primaryStore, "region-a/backup-1")
_, err := source.Backup(ctx, dataDir, hatBackup.BundleManifest{})
if err != nil {
	return err
}

// The object-store provider copies region-a/backup-1/* to region-b/backup-1/*.
replica, _ := hatBackup.NewObjectStoreTarget(secondaryStore, "region-b/backup-1")
if _, err := replica.Verify(ctx); err != nil {
	return err
}
_, err = replica.Restore(ctx, recoveryDir, false)
```

`Verify` streams the manifest and every referenced object, checking the
declared byte count and SHA-256 digest without allocating a restore tree or
publishing any files. Run it before `Restore` during a recovery drill. A
checksum or size mismatch stops the drill before the destination is changed.
The existing restore path still performs its own checks, so verification is an
early diagnostic rather than a replacement for restore validation.

The repeatable test covers backup, region-prefix transfer, verification,
restore, and post-transfer tampering:

```sh
make verify-object-store-verify
```

The operation does not perform cloud-provider replication, delete source
objects, or overwrite a destination. Those actions remain explicit operator
or provider responsibilities.
