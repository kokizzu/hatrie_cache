# T211: Configurable WAL Synchronization

The command journal now exposes an explicit synchronization mode and a
durability watermark. The default remains `periodic`, so existing callers keep
the previous behavior.

## Configuration

```go
journal, err := hatCache.OpenCommandJournalWithOptions(path, hatCache.CommandJournalOptions{
	SyncMode:            hatCache.CommandJournalSyncModeImmediate,
	GroupCommitMaxBatch: 64,
})
```

The cache package re-exports the modes:

| Mode | Behavior | Loss window |
| --- | --- | --- |
| `CommandJournalSyncModePeriodic` | Uses the configured group-commit behavior. | Records after the last successful sync can be lost on an abrupt failure. |
| `CommandJournalSyncModeImmediate` | Forces a sync boundary for every committed record, even when the configured batch is larger. | Minimal, subject to filesystem and device guarantees. |
| `CommandJournalSyncModeDisabled` | Skips explicit journal syncs. | All records since the last external durability boundary can be lost. |

`SyncMode` is normalized to `periodic` when it is zero. Invalid values are
rejected by `hatJournal.ValidateOptions`. The mode is process configuration;
it does not change the on-disk record format, so reopening an existing journal
with a different mode is supported.

## Durability reporting

`journal.DurabilityReport()` returns:

```go
report := journal.DurabilityReport()
// report.SyncMode
// report.Durable
// report.LastSequence
// report.LastSyncedSequence
// report.UnsyncedSequences
```

`Durable` is true only when the mode supports synchronization and the latest
appended sequence is covered by a successful sync. In disabled mode it is
always false when records exist. A successful sync advances
`LastSyncedSequence`; a failed sync does not. Recovered records are treated as
the current durable prefix for periodic and immediate modes because the
journal has already accepted them as the recovery source.

## Tradeoffs

- `periodic` is the compatibility and throughput-oriented default. A larger
  group batch can reduce sync calls when writers overlap, at the cost of a
  larger crash-loss window.
- `immediate` gives the strongest journal-level acknowledgement semantics, but
  can make storage latency and device write pressure the dominant cost.
- `disabled` is appropriate only when the journal is a disposable cache or an
  external checkpoint provides the recovery contract. It must not be used as a
  durability substitute.

The T211 benchmark uses a no-op sync hook to isolate mode dispatch and append
work; it does not claim to model physical `fsync` latency. See the raw output
and interpretation in [BENCHMARK.md](BENCHMARK.md#t211-wal-synchronization).

## Verification

The focused tests cover default normalization, invalid values, sync-hook call
counts, immediate-mode batch override, disabled-mode reporting, and sequence
watermarks. The standard repository cleanup target is run after test and
benchmark commands so generated temporary files do not accumulate in `/tmp`.
