# Durable Frontier-Based Backup

M-U50 adds a portable logical checkpoint envelope for backups that need one
consistent boundary across immutable storage, source positions, maintained
frontiers, and subscriptions. It is deliberately a control-plane contract;
the caller still owns the storage copy, source pause/barrier, journal, and
restore side effects.

## Manifest

`hatBackup.LogicalSnapshotManifest` records:

- the immutable `BundleBackupID` and `StorageGeneration`;
- the exact captured `JournalSequence` boundary;
- source/partition offsets and source epochs;
- lower/upper frontier values and generations;
- subscription as-of and acknowledged sequences.

`Normalize` validates IDs, duplicate records, frontier ordering, subscription
references, sequence bounds, and the total record limit. It returns independent
sorted slices, so callers can build a manifest in any order without making
serialization nondeterministic.

```go
manifest := hatBackup.LogicalSnapshotManifest{
	Version:           hatBackup.LogicalSnapshotManifestVersion,
	SnapshotID:        "snapshot-2026-09-19",
	CreatedAt:         time.Now().UTC(),
	BundleBackupID:    "backup-1042",
	StorageGeneration: 7,
	JournalSequence:   9001,
	SourceOffsets: []hatBackup.SourceOffsetCheckpoint{
		{SourceID: "orders", Partition: "0", Offset: 812, Epoch: 4},
	},
	Frontiers: []hatBackup.FrontierCheckpoint{
		{ID: "orders", Lower: 8998, Upper: 9001, Generation: 12},
	},
	Subscriptions: []hatBackup.SubscriptionCheckpoint{
		{ID: "orders-sub", FrontierID: "orders", AsOf: 8998, AckedSequence: 9001},
	},
}
payload, err := manifest.MarshalBinary()
```

The default codec is deterministic length-prefixed binary (`HLM1`), not JSON.
`DecodeLogicalSnapshotManifest` validates the complete payload before returning
it, and `UnmarshalBinary` does not mutate its receiver on failure. The encoded
manifest is bounded by `MaxLogicalSnapshotManifestBytes`; records are bounded
by `MaxLogicalSnapshotRecords`.

## Restore Order And Gap Detection

Call `PlanLogicalSnapshotRestore` before performing restore side effects. It
returns this fixed order:

1. Restore immutable storage.
2. Restore source offsets.
3. Restore frontiers.
4. Restore subscriptions.
5. Replay journal records after the captured boundary.

Pass the journal range available for replay. If the first available sequence
starts after `JournalSequence+1`, the plan returns
`ErrLogicalSnapshotHistoryGap` before any restore action. The caller should
rehearse the returned plan against a staging destination, verify bundle
checksums through the existing backup APIs, then persist the restored
checkpoint before admitting readers.

The API does not pause sources, acquire a distributed barrier, copy files,
replay a journal, or silently fill missing history. Those operations are
engine- and deployment-specific and must remain explicit.

## Benchmark

Commands:

```sh
make benchmark-m050-baseline
make benchmark-m050
```

The baseline runs against a clean `HEAD` archive and injects only a lower-bound
loop benchmark. Five runs on Linux/amd64, AMD Ryzen 9 5950X:

| Workload | ns/op samples | Median ns/op | B/op | allocs/op | Encoded size |
| --- | --- | ---: | ---: | ---: | ---: |
| Control loop | 0.5477, 0.5508, 0.5261, 0.5600, 0.5360 | 0.5477 | 0 | 0 | n/a |
| Marshal plus restore-plan validation | 2058, 1930, 1935, 1982, 2008 | 1982 | 1488 | 27 | 117 bytes |

The control loop is not an old implementation and its ratio to the feature is
not a product speedup. The feature intentionally pays a bounded allocation and
validation cost on backup/restore operations to obtain canonical ordering,
cross-record validation, and missing-history detection; it must not be placed
on a query or row-update hot path.
