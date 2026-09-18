# T-U36 Snapshot Rotation Policy

T-U36 adds an opt-in policy for scheduling incremental backup repository
rotations. It is inspired by database engines that separate snapshot cadence
from the write path and bound retained history by both age/change volume and
storage budget.

## Behavior

`BackupRotationPolicy` supports:

- `MaximumInterval`: force a backup after this elapsed time;
- `JournalSequenceDelta`: request a backup after this many new journal
  sequences;
- `MinimumInterval`: prevent journal-delta rotations from happening too often;
- `Retain`: count-based repository retention;
- `RetainBytes`: approximate unique backup-object byte budget.

The first backup is always due. A zero policy is invalid, so enabling the
helper is explicit. Existing `CreateBackupBundle` and ordinary journal writes
are unchanged.

```go
policy := hatriecache.BackupRotationPolicy{
    MaximumInterval:       24 * time.Hour,
    MinimumInterval:       5 * time.Minute,
    JournalSequenceDelta:  10000,
    Retain:                32,
    RetainBytes:           10 << 30,
}

result, err := hatriecache.CreateIncrementalBackupRepositoryIfDue(
    repositoryPath,
    trie,
    journal,
    hatriecache.BackupBundleOptions{
        PersistentStore: store,
        DirtyTracker:    tracker,
    },
    policy,
)
```

The helper creates no background goroutine. A deployment scheduler should call
it at its chosen cadence. A not-due call returns the current manifest and does
not write a new backup. The repository writer still serializes backup creation.

`RetainBytes` counts unique declared backup-object bytes across retained
manifests, so deduplicated objects are counted once. The newest manifest is
always kept even when it alone exceeds the budget. A zero byte budget preserves
the previous count-only behavior.

## Safety

The policy rejects negative durations and retention values, contradictory
intervals, journal sequence regression, and a clock moving backward relative
to the latest manifest. The policy decision has no filesystem side effects;
backup publication continues to use the existing atomic repository path.

## Measurement

Commands:

```text
make baseline-tu36-backup-rotation
make benchmark-tu36-backup-rotation
make test-tu36-backup-rotation
```

On Linux/amd64 with an AMD Ryzen 9 5950X, five `-benchmem` samples produced:

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op | Relative |
| --- | --- | ---: | ---: | ---: | --- |
| Equivalent inline cadence check, baseline | 9.368; 9.348; 8.907; 9.583; 9.078 | 9.348 | 0 | 0 | baseline |
| `BackupRotationPolicy.Decide`, after | 16.23; 18.02; 16.67; 16.36; 17.85 | 16.67 | 0 | 0 | 1.78x control cost; +7.322 ns |

The 7.415 ns absolute cost is paid only by the backup scheduler decision, not
by cache mutations, journal appends, or snapshot serialization. The feature is
therefore opt-in and does not claim a write-throughput improvement; its win is
bounded backup cadence and predictable retained storage.
