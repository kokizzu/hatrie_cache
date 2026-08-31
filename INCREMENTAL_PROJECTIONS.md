# Incremental Projections

`hatSql.IncrementalProjectionRunner` maintains selected materialized views from
an ordered mutation log. It is disabled by default. When disabled it neither
reads the journal nor evaluates a SQL refresh.

The runner deliberately coalesces a contiguous mutation tail into one
source-consistent materialized-view refresh. Cache mutations can replace an
arbitrary JSON document, so they do not always contain safe row-level
before/after data for a generic `COUNT`, `SUM`, or `GROUP BY` delta. This gives
correct, at-least-once view maintenance without inventing unsafe row deltas.

## Journal Setup

Use `hatCache.SQLJournalProjectionRunner` with an existing `CommandJournal`.
Its dependencies are CACHE keys, matching `MaterializedViewDefinition`.

```go
package main

import (
	"context"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatSql"
)

func startProjection(ctx context.Context, trie *hatCache.HatTrie, journal *hatCache.CommandJournal) error {
	views := hatSql.NewMaterializedViews()
	_, err := views.Create(ctx, hatSql.MaterializedViewDefinition{
		Name:         "team_totals",
		Query:        "FROM CACHE('events') SELECT team, COUNT(*) AS total GROUP BY team",
		Dependencies: []string{"events"},
	}, trie, hatSql.QueryOptions{})
	if err != nil {
		return err
	}

	checkpoints, err := hatSql.NewFileProjectionCheckpointStore("/var/lib/hatrie-cache/projections.json")
	if err != nil {
		return err
	}
	runner, err := hatCache.NewSQLJournalProjectionRunner(views, trie, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{
		Name:            "team_totals",
		Enabled:         true,
		CheckpointStore: checkpoints,
	})
	if err != nil {
		return err
	}
	_, err = runner.RunOnce(ctx, journal, 128)
	return err
}
```

Call `RunOnce` from the service's controlled worker loop. The `limit` is a
bounded journal read, not a background worker count. A value such as `128`
keeps one run bounded while still coalescing ordinary write bursts. The runner
uses the journal sequence only after journaled commands have been applied to
the cache source.

## Optional Journal Retention Protection

Snapshots normally compact journal records through the snapshot boundary. A
projection that must survive snapshots while it is temporarily behind can opt
in to a named journal watermark. This is off by default because retained log
records consume disk space until the projection catches up.

```go
runner, err := hatCache.NewSQLJournalProjectionRunnerWithOptions(views, trie, hatSql.QueryOptions{}, hatCache.SQLJournalProjectionRunnerOptions{
	Incremental: hatSql.IncrementalProjectionRunnerOptions{
		Name:            "team_totals",
		Enabled:         true,
		CheckpointStore: checkpoints,
	},
	ProtectJournalRetention: true,
})
if err != nil {
	return err
}
defer runner.RemoveJournalRetention(journal)
```

Before each enabled `RunOnce`, the runner registers its current checkpoint;
after a successful refresh it advances the watermark. `CommandJournal` will
not compact beyond the slowest active watermark. A protected runner therefore
retains unapplied records across snapshots, while every existing constructor
keeps the prior compaction behavior. Call `RemoveJournalRetention` when the
runner is permanently stopped so an abandoned name cannot retain the journal.
Protection is in-memory journal state; after a process restart the runner
registers its durable checkpoint on its first enabled run.

## Checkpoints And Recovery

`FileProjectionCheckpointStore` stores named checkpoints in an atomically
replaced JSON file. It creates files with mode `0600`, syncs the file and
parent directory, and rejects symlinked checkpoint paths. Keep the parent
directory owned by the service account and outside the journal retention path.

After a successful refresh, the runner saves the checkpoint. If saving fails,
the in-memory checkpoint does not advance; the next run may repeat the refresh.
That is intentional at-least-once behavior and is safe because each refresh
publishes a complete immutable view snapshot.

If `CommandJournal.Tail` reports that the stored checkpoint was compacted:

1. Restore or catch up the source cache to a trusted snapshot/journal boundary.
2. Call `runner.Rebuild(ctx, dependencies, boundarySequence)` with that same
   boundary sequence.
3. Resume `RunOnce` normally. The next tail begins at `boundarySequence + 1`.

Never adopt a later projection checkpoint than the source snapshot. Doing so
would skip mutations that the materialized view has not incorporated.

## Guarantees And Limits

- The default is off. Enabling is explicit through `Enabled: true`.
- Input changes above the checkpoint must be contiguous. A gap is rejected
  rather than silently advancing the checkpoint.
- Replayed records at or below the checkpoint are ignored.
- A failed source refresh leaves the previous materialized-view snapshot and
  checkpoint unchanged.
- This mechanism is for repeated materialized SQL queries. It does not improve
  direct cache `GET`/`SET` or arbitrary ad-hoc SQL.
- Kafka is not required. `CommandJournal` is the local ordered log. An external
  queue adapter can supply `hatSql.ProjectionChange` values later without
  changing the runner contract.

## Verification

```sh
make test-sql-incremental-projection
make test-sql-projection-retention
make benchmark-sql-incremental-projection
```
