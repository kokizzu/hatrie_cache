# Transactional SQL Triggers

`hat/hatSql` exposes an importable transaction trigger coordinator for code
that already owns a SQL mutation path. It also parses and registers a strict
row-level `CREATE TRIGGER` definition through `RegisterSQLTrigger`; callers
still stage row events and provide the primary storage participant, receiving
deterministic prepare, commit, and rollback behavior.

```go
registry := hatSql.NewSQLTriggerRegistry()
_ = registry.Register(hatSql.SQLTrigger{
	Name: "audit-insert", Source: "people", Operation: "INSERT", Order: 10,
	Prepare: func(ctx context.Context, event hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
		return hatSql.SQLTriggerAction{
			Commit: func(context.Context) error {
				return auditLog.Append(event.Key)
			},
			Rollback: func(context.Context) error {
				return auditLog.Remove(event.Key)
			},
		}, nil
	},
})

transaction, err := registry.BeginSQLTriggerTransaction(ctx)
if err != nil {
	return err
}
if err := transaction.Add(hatSql.SQLTriggerEvent{
	Source: "people", Operation: "INSERT", Key: "ada", After: row,
}); err != nil {
	return err
}
return transaction.Commit(func(context.Context, []hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
	return tableMutation.Prepare(row)
})
```

The SQL definition path accepts `CREATE TRIGGER name AFTER INSERT|UPDATE|DELETE|REPLACE
ON source FOR EACH ROW`, with an optional trailing semicolon. The parser
rejects `BEFORE`, statement-level triggers, unsupported operations, and
additional statements. Registration remains explicit so an application can
bind the callback to its own storage and side-effect policy:

```go
if err := registry.RegisterSQLTrigger(
	"CREATE TRIGGER audit AFTER INSERT ON people FOR EACH ROW",
	prepareAudit,
); err != nil {
	return err
}
```

Triggers are sorted by ascending `Order`, then name. Events retain their add
order. Matching is exact for non-empty `Source` and `Operation`; an empty
filter is a wildcard. All matching trigger preparation callbacks run before
the primary mutation is prepared. Commit runs the primary action first and
then triggers in the same deterministic order. Any preparation or commit
failure rolls back every prepared action in reverse order. Rollbacks must be
idempotent because a commit action may have run before a later action fails.

`SQLTriggerAction` callbacks are the atomicity boundary. A callback that has
already changed an external system without returning a compensating rollback
cannot be made transactional by this package. The existing SQL parser and
typed-table mutation methods are unchanged, and no trigger registry is
created by default.

## Automatic DML Dispatch

`ExecuteSQLMutation` can dispatch registered row-level triggers automatically
when `SQLQueryOptions.TriggerRegistry` is set:

```go
registry := hatSql.NewSQLTriggerRegistry()
_ = registry.Register(hatSql.SQLTrigger{
	Name: "audit", Source: "cache", Operation: "INSERT",
	Prepare: func(ctx context.Context, event hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
		return hatSql.SQLTriggerAction{}, nil
	},
})
result, err := hatCache.ExecuteSQLMutation(ctx, trie, query, nil, hatCache.SQLQueryOptions{
	TriggerRegistry: registry,
})
```

This is opt-in; a nil registry preserves the existing caller-owned behavior.
The automatic path covers one key-targeted `INSERT`, `UPDATE`, or `DELETE`
without expiration fields. An `INSERT` replacing an existing key emits
`REPLACE`. Events use source `cache`, include the key and string value when
available, and carry before/after snapshots. Trigger preparation runs before
the primary mutation; trigger commit failure rolls back the primary mutation
for non-expiring string and counter keys. `MERGE`, `ON CONFLICT`, `INSERT ...
SELECT`, atomic multi-statement batches, expiration mutations, and composite
values are rejected while automatic dispatch is enabled instead of silently
running without triggers.

The enabled path intentionally allocates transaction/event state. On the local
benchmark fixture, one no-op trigger had a median `2.68x` CPU cost, `2.45x`
allocated bytes, and 64 additional allocations; see
[BENCHMARK.md](BENCHMARK.md#automatic-sql-dml-trigger-dispatch). This cost is
absent from the default path.

## Current Boundary

The parser and registration helper cover the safe row-level DDL shape, but DML
wiring remains caller-owned. The package does not execute trigger bodies,
create a registry by default, or replicate trigger definitions across
processes; those require an explicit storage transaction contract and failure
policy for external side effects.

## Verification

```text
make test-sql-triggers-full-clean
make test-sql-triggers-race-clean
make benchmark-sql-triggers-clean
```
