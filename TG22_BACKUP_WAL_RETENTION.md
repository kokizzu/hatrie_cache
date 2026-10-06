# T-G22: Backup WAL Retention Leases

Status: adopted, opt-in.

T-G21 creates a consistent snapshot and records its journal coordinate, but a
slow transfer can outlive a later compaction request. T-G22 keeps the journal
records after that coordinate available until the transfer or replay consumer
has finished.

## Usage

`CreateHotBackupBundle` and its context variant acquire and release the lease
automatically around snapshot streaming and atomic bundle publication. Custom
snapshot transfer code should use the atomic API:

```go
manifest, lease, err := journal.WriteSnapshotWithManifestAndBackupRetentionLease(
	trie,
	writer,
	SnapshotFormatBinary,
)
if err != nil {
	return err
}
defer lease.Release()

// Transfer manifest and journal records after manifest.JournalSequence.
_ = manifest
```

`AcquireBackupRetentionLease(sequence)` is available when a caller already
owns an exact, safely captured journal coordinate. `Release` is idempotent and
returns true only for the first release of an active lease.

## Semantics

- A lease protects records *after* its sequence. The snapshot coordinate
  itself may be compacted because restore starts from that snapshot.
- Compaction uses the oldest active backup lease and the oldest projection
  watermark, so either consumer can hold the boundary back.
- Both single-file and segmented journal pruning honor the lease.
- Leases are process-local and not persisted. Reacquire one after restart or
  restore before starting a transfer that depends on the live journal.
- There is no automatic timeout. A leaked lease can retain WAL indefinitely;
  release it on success, cancellation, and failure paths.
- With no active lease, no lease map is allocated and normal journal behavior
  remains unchanged. The feature is not enabled by configuration or by
  default.

## Tradeoff

The benefit is correctness for slow online backup and replay consumers. The
cost is retained WAL while a lease is active and one in-memory lease object per
active transfer. It does not make compaction faster and it intentionally does
not delete or rotate retained data automatically.

The focused benchmark compares the retention-boundary lookup with zero, one,
and four active leases:

```text
make benchmark-tg22
```

The benchmark is recorded in `BENCHMARK.md` when the package build is
available. In the current worktree, the target is blocked by pre-existing
missing `hat/hatSql` symbols (`MaxDataflowTextBytes`, `TypedTableDate`, and
`TypedTableTimestamp`), so no performance win is claimed.
