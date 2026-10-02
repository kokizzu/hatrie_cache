# T-U05 Transaction Session Settings

`hatCache.SQLTransactionSession` stores defaults for transactions created by one client or connection session. The defaults are copied when `Begin` is called, so changing a session does not alter transactions that are already open.

```go
session, err := hatCache.NewSQLTransactionSessionWithOptions(trie, hatCache.SQLTransactionOptions{
	Isolation: hatCache.SQLTransactionIsolationSerializable,
	ReadOnly:  true,
	Timeout:   30 * time.Second,
})
if err != nil {
	return err
}

transaction, err := session.Begin()
if err != nil {
	return err
}
defer transaction.Rollback()

result, err := transaction.Query(ctx, `SELECT key, value FROM cache`, nil, hatCache.SQLQueryOptions{})
```

Use `SetOptions` to replace defaults for future transactions. Invalid isolation values and negative timeouts are rejected before the session changes. `NewSQLTransactionSession` selects the existing snapshot, writable, unlimited-time defaults.

Durability is deliberately not part of this wrapper. Callers configure and flush the `PersistentStore` separately because the session does not own storage, journals, or backup lifecycle.
