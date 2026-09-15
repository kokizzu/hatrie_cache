# Verified Object-Store Garbage Collection

`hatBackup.ObjectStoreTarget` now supports opt-in garbage collection for
content-addressed incremental backups when the backend implements the optional
`ObjectStoreObjectLister` and `ObjectStoreObjectDeleter` interfaces.

## Safe Workflow

1. Load the durable manifest catalog and create a `BackupRetentionPlan` with
   `PlanBackupRetention`.
2. Call `PlanGarbageCollection` to list physical objects below the target's
   `objects/` prefix.
3. Review and persist the exact `DeleteObjectKeys` list.
4. Call `ApplyGarbageCollection` with that unchanged plan.

```go
manifests, err := catalog.Load()
if err != nil {
	return err
}
retention, err := hatBackup.PlanBackupRetention(manifests, latestBackupID, 2)
if err != nil {
	return err
}
plan, err := target.PlanGarbageCollection(ctx, retention)
if err != nil {
	return err
}
// Review or persist plan.DeleteObjectKeys before applying it.
_, err = target.ApplyGarbageCollection(ctx, plan)
```

The planner validates the complete incremental chain, requires every chain
manifest to use content-addressed layout, derives the retained object set from
the retained manifest suffix, and refuses to return a plan if a retained
object is absent from the backend listing. Only canonical physical keys below
the target prefix are eligible for deletion. Unknown names under `objects/`
are reported in `SkippedObjectKeys` and left untouched.

## Backend Contract And Limits

The base `ObjectStore` interface remains unchanged. Stores without optional
list/delete support continue to provide backup and restore and return an
explicit unsupported error for GC. Apply is an exact, idempotent plan executor
when the backend treats missing deletes as success; a backend failure can leave
a partial deletion, so operators should rerun planning after errors.

The catalog and object listing must be coordinated with backup writers by the
caller. This feature does not provide a distributed lock, a timestamp frontier
lease, or an atomic batch delete. Those are intentionally still open parts of
the broader Materialize-style blob-GC design.

## Verification

```text
make test-mz006-c228
make test-mz006-package-c228
make race-mz006-c228
make race-mz006-package-c228
make vet-mz006-c228
make benchmark-mz006-c228
```

Tests cover orphan deletion, retained-object protection, unknown-object
preservation, traversal/wrong-prefix rejection, missing keep objects, and
backends without optional capabilities.
