# TT-011 Point-in-Time Snapshot Restore

Point-in-time restore is an opt-in recovery mode for snapshot backup bundles
that include a command journal. It restores the snapshot and keeps only journal
records through one committed journal sequence, so operators can recover to a
known state before a bad write or application rollout.

The default is unchanged. `MaxJournalSequence: 0` performs the normal complete
bundle restore and the CLI flag defaults to `0`.

## CLI

Restore through sequence `123` with the existing Makefile CLI wrapper:

```
make cli ARGS='restore-bundle -bundle backup/run-001.tar.gz -data-dir data/recovered -max-journal-sequence 123'
```

The destination must be a clean restore directory under the normal backup
workflow. The source bundle is never modified. The restored journal is
atomically replaced with the prefix through the selected sequence.

## Go API

Embedded callers set `MaxJournalSequence` on
`BackupBundleRestoreOptions`:

```go
report, err := hatCache.RestoreBackupBundle(
    bundlePath,
    destination,
    hatCache.BackupBundleRestoreOptions{MaxJournalSequence: 123},
)
```

The returned report records the effective restored journal sequence. A caller
can load the restored snapshot and replay its journal in the same way as a
normal restore.

## Constraints

- The option requires a snapshot backup bundle. Repository and Pebble
  checkpoint restore modes reject it because they do not have a snapshot
  replay boundary in this feature.
- The target must be at least the snapshot checkpoint and no later than the
  sequence recorded by the bundle manifest.
- A target after the snapshot checkpoint must be an exact committed journal
  sequence present in the bundle. The snapshot checkpoint itself is valid and
  produces a snapshot-only recovery point.
- JSON and binary command-journal formats are supported.
- A target of zero keeps the complete journal tail and preserves existing
  restore behavior.
- Partition-scoped restore can combine its existing key-prefix selection with
  the sequence limit. Unsupported complex tail records remain rejected by the
  selective-restore rules.

The journal is validated before publication, and the destination staging tree
is published atomically by the existing restore workflow. Keep bundle files
and journal contents protected because command journals can contain caller
keys, values, and idempotency material.

## Verification

The focused tests cover both journal formats, replayed state, the checkpoint
boundary, invalid lower and upper bounds, and CLI propagation:

```
make test-tt011-point-in-time-restore-c291
make verify-tt011-point-in-time-restore-c291
```

The benchmark targets use a clean archive and `-benchmem -benchtime=10x
-count=5`:

```
make benchmark-before-tt011-point-in-time-restore-c291
make benchmark-tt011-point-in-time-restore-c291
```

See [BENCHMARK.md](BENCHMARK.md#tt-011-point-in-time-snapshot-restore) for
the recorded raw output and tradeoff analysis.
