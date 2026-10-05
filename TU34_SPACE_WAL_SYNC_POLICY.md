# Per-Space WAL Sync Policy

`CommandJournal` keeps immediate WAL syncing as the default. Applications that
have a separate durability boundary can opt in to a policy for a logical space
after opening the journal:

```go
if err := journal.SetSpaceSyncPolicy("telemetry", hatCache.CommandJournalSpaceSyncPolicy{
	Mode:  hatCache.CommandJournalSpaceSyncPeriodic,
	Every: 64,
}); err != nil {
	return err
}
```

The package exposes three modes:

| Mode | Behavior | Durability tradeoff |
| --- | --- | --- |
| `CommandJournalSpaceSyncImmediate` | `fsync`-style sync for every accepted journal entry. This is the default for every unconfigured space. | Lowest crash-loss window; highest sync cost. |
| `CommandJournalSpaceSyncPeriodic` | Sync once every `Every` accepted entries for that space. A group or record batch performs one sync when any member reaches its cadence. | A crash can lose recent entries since the last cadence boundary. |
| `CommandJournalSpaceSyncDisabled` | Does not request a sync for that space. | Entries can remain lost after a process or host crash until the caller establishes a durability boundary. |

Use `journal.Sync()` for an explicit boundary, such as before acknowledging an
external checkpoint or handoff. `journal.Close()` closes the file but does not
add an `fsync` guarantee. `journal.SpaceSyncPolicies()` returns a copy of the
configured overrides for diagnostics.

Policy details:

- The configuration is in-memory and must be applied again after reopening a
  journal.
- `Every` must be greater than zero for periodic mode. `Every: 1` is equivalent
  to immediate mode for that space.
- Failed validation is rejected before a journal append. Append failures and
  rejected execution restore periodic cadence state; rollback cleanup still
  uses the journal's normal sync path.
- The policy applies to direct commands, group commit, idempotent group commit,
  replicated record batches, compacted record batches, and prepared internal
  replication commands.
- A batch containing an immediate or due-periodic entry syncs once for the
  batch. A batch containing only disabled entries skips that sync.
- This controls WAL sync frequency only. It does not change journal format,
  encryption, replay order, backup contents, or replication authorization.

The feature is intentionally opt-in because disabled and periodic modes weaken
crash durability. The benchmark section in [BENCHMARK.md](BENCHMARK.md#t-u34-per-space-wal-sync-policy)
contains raw samples and the CPU, allocation, and sync-count tradeoff.
