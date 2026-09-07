# Transactional SQL Triggers

`hat/hatSql` exposes an importable transaction trigger coordinator for code
that already owns a SQL mutation path. It is deliberately separate from the
SQL parser: callers stage row events, provide the primary storage participant,
and receive deterministic prepare, commit, and rollback behavior.

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

This is the reusable transaction primitive behind future SQL trigger syntax;
it does not add `CREATE TRIGGER`, automatic DML wiring, or cross-process
trigger replication. Those remain separate work because they need a storage
transaction contract and an explicit failure policy for external side
effects.

## Verification

```text
make test-sql-triggers-full-clean
make test-sql-triggers-race-clean
make benchmark-sql-triggers-clean
```
