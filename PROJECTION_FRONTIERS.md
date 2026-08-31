# Projection Retention Frontiers

`hatCache.SQLProjectionRetentionFrontier` coordinates journal retention for a
set of separately maintained SQL projections. It is disabled by default and
does not change `SQLJournalProjectionRunner` behavior.

Use it when multiple projections must all process a batch before journal
compaction can discard that batch. The frontier pins one journal watermark at
the lowest completed source sequence. It is a retention and recovery boundary,
not a distributed transaction or a historical cross-source snapshot.

## Setup

Each runner needs its own durable `CheckpointStore`, must be enabled, and must
leave `ProtectJournalRetention` disabled. The shared frontier owns retention.

```go
frontier, err := hatCache.NewSQLProjectionRetentionFrontier("analytics", []string{
	"orders",
	"people",
})
if err != nil {
	return err
}

peopleRunner, err := hatCache.NewSQLJournalProjectionRunner(views, trie, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{
	Name:            "people",
	Enabled:         true,
	CheckpointStore: peopleCheckpoints,
})
if err != nil {
	return err
}
ordersRunner, err := hatCache.NewSQLJournalProjectionRunner(views, trie, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{
	Name:            "orders",
	Enabled:         true,
	CheckpointStore: ordersCheckpoints,
})
if err != nil {
	return err
}

defer frontier.Remove(journal)
_, err = frontier.RunOnce(ctx, journal, 128, map[string]*hatCache.SQLJournalProjectionRunner{
	"people": peopleRunner,
	"orders": ordersRunner,
})
return err
```

`RunOnce` writes the current frontier watermark before it starts. It runs every
configured runner, then commits the lowest runner checkpoint only when every
runner succeeds. If any runner fails, the old watermark remains, so a later
snapshot cannot compact past a partially completed coordinated batch.

## Recovery

Persist each runner checkpoint with `hatSql.FileProjectionCheckpointStore` as
described in [INCREMENTAL_PROJECTIONS.md](INCREMENTAL_PROJECTIONS.md). A
frontier's journal watermark is in-memory protection, so a restarted process
must invoke `RunOnce` again to register its durable progress before allowing a
snapshot to compact the journal.

The application must use a stable name and the same exact source set after a
restart. A changed source set is a new frontier: rebuild affected projections
from a trusted source snapshot and journal sequence before enabling it.

## Limits

- A source name must occur exactly once; missing, extra, or regressing
  checkpoints are rejected without advancing retention.
- `RunOnce` rejects runners with `ProtectJournalRetention: true`; independent
  watermarks could otherwise advance beyond the shared frontier.
- This does not make independently refreshed views transactionally consistent.
  It protects recovery retention. Use one materialized view and one runner for
  a query that requires a single coalesced source refresh.
- Call `Remove` when the coordinated projections are permanently stopped. An
  abandoned frontier can retain journal records indefinitely.

## Verification

```sh
make test-sql-projection-frontier
make verify-sql-projection-frontier
make benchmark-sql-projection-frontier
```
