# T-G18 WAL Sync Policy

This is the Tarantool-inspired WAL durability policy for the command journal.
It makes the existing filesystem-sync boundary explicit without changing the
safe default.

## Modes

`durable` is the default and preserves the existing behavior: a successful
journal append waits for a filesystem sync. `periodic` performs the first sync
immediately, then syncs again when the configured interval has elapsed. `none`
does not automatically sync successful appends. Both non-durable modes are
explicit opt-ins; they can lose the most recent acknowledged writes after a
process or host failure.

The default periodic interval is `100ms`. It is only applied when
`periodic` is selected. Valid intervals are `1ms` through `1h`.

An embedded caller can configure the policy through `hatJournal.Options` or
the compatibility aliases in `hatCache.CommandJournalOptions`:

```go
journal, err := hatCache.OpenCommandJournalWithOptions(path, hatCache.CommandJournalOptions{
    Format:       hatCache.DefaultCommandJournalFormat,
    SyncMode:     hatCache.CommandJournalSyncModePeriodic,
    SyncInterval: 100 * time.Millisecond,
})
```

`CommandJournal.Sync()` is an explicit durability barrier and forces a sync in
every mode. Periodic mode also flushes pending writes on graceful close.
Rollback paths force a barrier before returning an append error so a failed
write cannot leave an uncommitted truncation behind.

## CLI

```sh
# Safe default; this is equivalent to omitting both flags.
hatrie-cache -journal-path data/commands.journal -journal-sync-mode durable

# Explicit latency/durability tradeoff.
hatrie-cache -journal-path data/commands.journal \
  -journal-sync-mode periodic -journal-sync-interval 100ms

# Only for workloads that accept crash-loss risk.
hatrie-cache -journal-path data/commands.journal -journal-sync-mode none
```

The configuration is included in the redacted startup configuration. The
policy applies to the journal opened by that process; it does not change
replication acknowledgements, backup verification, or restore semantics.

## Why This Is Bounded

No background sync goroutine or per-record timer is added. Periodic mode
checks the interval only on an append, and callers with an idle journal can
call `Sync()` at an application-defined commit boundary. Durable mode keeps
the previous sync call and allocation profile.
