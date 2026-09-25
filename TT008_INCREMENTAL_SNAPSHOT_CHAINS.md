# TT-008: Incremental Snapshot Chains

TT-008 is adopted as an opt-in Tarantool-inspired backup chain for the
content-addressed object-store path. A backup manifest can point at one parent
manifest, and the durable `BackupManifestCatalog` verifies that parent-linked
chains are complete before exposing them to callers.

## Usage

```go
catalog, err := hatBackup.NewBackupManifestCatalog("/var/lib/hatrie/backup/manifests.log")
if err != nil {
	return err
}

target, err := hatBackup.NewObjectStoreTargetWithOptions(store, "cache-backups", hatBackup.ObjectStoreTargetOptions{
	Layout:          hatBackup.ObjectStoreLayoutContentAddressed,
	ManifestCatalog: catalog,
})
if err != nil {
	return err
}

base, err := target.Backup(ctx, sourceDir, hatBackup.BundleManifest{
	Mode:             hatBackup.ModePebbleIncremental,
	BackupID:         "base-001",
	Store:            "cache",
	StorageBackend:   "pebble",
	StorageFormat:    "v1",
	StorageIdentity:  "cache-primary",
	StorageGeneration: 7,
	JournalSequence:  100,
})
if err != nil {
	return err
}

latest, err := target.Backup(ctx, sourceDir, hatBackup.BundleManifest{
	Mode:             hatBackup.ModePebbleIncremental,
	BackupID:         "delta-002",
	ParentBackupID:   base.BackupID,
	Incremental:      true,
	Store:            "cache",
	StorageBackend:   "pebble",
	StorageFormat:    "v1",
	StorageIdentity:  "cache-primary",
	StorageGeneration: 7,
	JournalSequence:  120,
})
if err != nil {
	return err
}

plan, err := catalog.Plan(latest.BackupID)
if err != nil {
	return err
}
retention, err := hatBackup.PlanBackupRetention(plan.Manifests, latest.BackupID, 2)
if err != nil {
	return err
}
_ = retention
```

`ModePebbleIncremental` selects content-addressed storage when the layout is
left at `auto`. Supplying `ManifestCatalog` makes parent metadata durable; it
is otherwise caller-owned and disabled. `ModeSnapshot` and
`ModePebbleCheckpoint` keep their existing behavior.

## Guarantees

- The first manifest is a complete base and cannot be marked incremental.
- Every child names an existing parent, and a child must be marked incremental.
- Chain planning rejects missing or cyclic parents, sequence regressions,
  mixed storage identities/generations, malformed paths or hashes, and
  conflicting object sizes.
- Content-addressed files are uploaded once per object hash and reused by later
  manifests. Optional fixed-size chunks use the same verified object identity.
- Retention protects every object referenced by a kept manifest before listing
  deletions.
- The latest manifest contains the complete checkpoint file references, so
  restore does not replay every parent. The chain planner is used for verified
  selection, retention, and operator-controlled recovery workflows.

## Tradeoffs

The mode still scans and hashes source files on every backup. An unchanged
backup therefore avoids payload writes but retains hashing and manifest work.
The first backup and backups containing new content pay the full upload cost;
the catalog also adds a durable append. Retained history consumes metadata and
object-store space, so retention should be applied explicitly.

## Verification And Measurements

Focused verification targets:

```text
make test-backup-catalog-c203
make test-ch022-incremental-part-backup-c203
make race-backup-catalog-c203
make vet-backup-catalog-c203
make benchmark-backup-catalog-c203
make benchmark-ch022-incremental-part-backup-c203
make benchmark-tt008-chain
```

Latest five-sample medians on Linux amd64, AMD Ryzen 9 5950X:

| Workload | ns/op | B/op | allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Plan 128-manifest chain | 110,031 | 194,745 | 397 | Validation and ordering cost |
| Catalog append | 739,314 | 3,656 | 20 | Durable chain metadata append |
| Catalog load | 275,162 | 150,593 | 870 | Load and validate catalog |
| 1 MiB path-layout backup | 1,978,877 | 2,719,414 | 1,614 | 1 MiB payload, 32 puts |
| 1 MiB content-addressed unchanged | 1,251,922 | 112,470 | 1,182 | Zero payload bytes and puts; 1.58x faster, 24.18x lower heap |
| 1 MiB content-addressed new | 2,979,599 | 2,775,668 | 2,193 | 1 MiB payload, 32 puts; 1.51x slower than path layout |

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#tt-008-incremental-snapshot-chains).
