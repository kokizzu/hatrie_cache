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
