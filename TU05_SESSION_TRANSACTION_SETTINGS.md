# T-U05 Session Transaction Settings

`hatSql.SQLSessionTransactionSettings` provides one validated, copy-safe
settings contract for an SQL session:

- `Timeout` is applied by `SQLSession.Execute`. A caller deadline that is
  earlier still wins. Zero keeps the existing unlimited session behavior.
- `ReadOnly` rejects session-local table, named-result, view, and projection
  mutations. Use `DropTemporaryTableChecked` when the caller needs an error;
  the older `DropTemporaryTable` method remains a compatibility wrapper.
- `Isolation` and `Durability` are validated metadata for an embedding
  transaction adapter. The current SQL session has no general transaction
  engine, so it does not pretend to enforce those two fields.

Use `NewSQLSessionWithTransactionSettings` for construction, or use
`SetTransactionSettings` and `ResetTransactionSettings` at a session boundary.
`TransactionSettings` returns a value copy. Settings reads use an immutable
atomic pointer, so the default query path does not take the session mutex and
does not allocate.

```go
session, err := hatSql.NewSQLSessionWithTransactionSettings(source,
    hatSql.SQLSessionTransactionSettings{
        Isolation:  hatSql.SQLSessionTransactionIsolationRepeatableRead,
        Timeout:    2 * time.Second,
        ReadOnly:   true,
        Durability: hatSql.SQLSessionTransactionDurabilityImmediate,
    },
)
if err != nil {
    return err
}
defer session.ResetTransactionSettings()
result, err := session.Execute(ctx, `FROM CACHE('events') SELECT id`, nil, hatSql.SQLQueryOptions{})
```

The settings setter is not an authorization boundary. An HTTP, gRPC, or
embedded service must authorize who may change a session's transaction policy.
Isolation and durability still require the caller's transaction/WAL adapter;
this feature deliberately keeps that integration explicit and default-off.
