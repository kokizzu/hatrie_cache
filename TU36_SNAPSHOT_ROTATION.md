# T-U36 Snapshot Rotation Policy

T-U36 adds opt-in snapshot rotation controls without changing ordinary journal
writes, backup creation, or restore defaults. The feature has two layers: the
existing scheduler helper decides whether an incremental repository backup is
due, while `hat/hatBackup` exposes a deterministic dry-run retention planner.

The planner does not delete files, mutate an object store, schedule backups,
or start a background goroutine.

## Existing scheduler integration

`BackupRotationPolicy` supports the scheduler-level controls:

- `MaximumInterval` forces a backup after the configured elapsed time.
- `JournalSequenceDelta` requests a backup after enough new journal
  sequences.
- `MinimumInterval` prevents journal-delta rotations from happening too
  often.
- `Retain` provides count-based repository retention.
- `RetainBytes` provides an approximate unique backup-object byte budget.

The first backup is always due. A zero policy is invalid, so enabling the
helper is explicit. A deployment scheduler calls
`CreateIncrementalBackupRepositoryIfDue` at its chosen cadence; a not-due
call returns the current manifest and does not write a new backup. The
repository writer still serializes backup creation.

`RetainBytes` counts unique declared backup-object bytes across retained
manifests, so deduplicated objects are counted once. The newest manifest is
always kept even when it alone exceeds the budget. A zero byte budget preserves
the previous count-only behavior.

The scheduler policy rejects negative durations and retention values,
contradictory intervals, journal sequence regression, and a clock moving
backward relative to the latest manifest. Its decision has no filesystem side
effects; backup publication continues to use the existing atomic repository
path.

## API

```go
policy := hatBackup.BackupRotationPolicy{
	MaxBackups:     14,
	MaxAge:         30 * 24 * time.Hour,
	MaxObjectBytes: 500 << 30,
	Now:            time.Now(),
}
plan, err := hatBackup.PlanBackupRotation(manifests, latestBackupID, policy)
```

The returned `BackupRetentionPlan` contains:

- `KeepBackupIDs` and `DeleteBackupIDs` for manifest retention.
- `KeepObjectKeys` and `DeleteObjectKeys` for content-addressed storage.
- `KeepObjectBytes` for the unique object bytes still required by retained
  manifests.
- The existing hash lists for compatibility with hash-addressed callers.

The caller must verify the plan against its current storage state and apply
deletions using its own locking and retry policy. The planner is safe to use
in a preview or admission check before an actual rotation.

## Policy semantics

- `MaxBackups`, `MaxAge`, and `MaxObjectBytes` are independent constraints.
- A zero limit disables that constraint. The all-zero policy keeps the full
  validated chain.
- The latest validated backup is always kept. If that backup alone is larger
  than `MaxObjectBytes`, the function returns
  `ErrBackupRotationBudgetExceeded` instead of producing an unsafe plan.
- Candidates are visited newest first. A candidate older than `Now-MaxAge`
  is skipped when its `CreatedAt` is known; a zero timestamp is retained
  conservatively because its age cannot be proven.
- The byte limit counts unique object identities, not repeated references
  from multiple manifests. Encrypted content-addressed manifests use their
  derived object keys, so different encryption key IDs are not conflated.
- Negative limits return `ErrBackupRotationInvalid`. `Now` is converted to
  UTC; a zero `Now` uses the current UTC time.
- Chain validation still rejects missing or cyclic parents, unsafe paths,
  storage-generation mismatches, sequence regressions, malformed hashes, and
  conflicting object sizes.

The policy is intentionally opt-in. Existing `PlanBackupRetention` remains
the lower-overhead count-only planner, and no default backup or restore path
changes its behavior.

## Measured cost

The benchmark uses 128 manifests, a requested retention of 32, a fixed clock,
and `-benchtime=200ms -count=5`. The baseline is the existing count-only
`PlanBackupRetention`; the feature measurement is `PlanBackupRotation` with
the same chain and active count/age/byte constraints. Values are medians from
the five samples.

| Planner | ns/op | B/op | allocs/op | Relative CPU | Relative bytes | Relative allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `PlanBackupRetention` (clean HEAD) | 116,820 | 206,528 | 444 | 1.00x | 1.00x | 1.00x |
| `PlanBackupRotation` (feature) | 143,976 | 215,545 | 507 | 1.23x | 1.04x | 1.14x |

This is additive planning work for callers that request age and byte-budget
constraints; it is not a replacement for the existing fast count-only path.
The measured overhead buys deterministic age selection, a unique-byte budget,
latest-backup protection, and object-key deletion planning. No automatic
deletion is enabled by this feature. The five clean-HEAD baseline samples were
116,711, 116,820, 108,812, 121,934, and 132,368 ns/op; the five feature
rotation samples were 140,269, 150,979, 145,749, 143,976, and 137,713 ns/op.

## Verification

Focused tests cover combined count/age selection, unique-object byte budgets,
latest-backup budget failure, invalid limits, and deterministic deletion
lists. Run the package tests and race checks before applying a returned plan:

```text
go test ./hat/hatBackup
go test -race ./hat/hatBackup -count=1
go vet ./hat/hatBackup
```

The scheduler decision benchmark remains separate from the manifest planner.
On Linux/amd64, five samples measured the equivalent inline cadence check at a
median of 9.348 ns/op and `BackupRotationPolicy.Decide` at 16.67 ns/op, with
zero allocations in both cases. The 7.322 ns absolute cost is paid only when
the opt-in scheduler asks for a decision; it is not added to cache mutations,
journal appends, or snapshot serialization.
