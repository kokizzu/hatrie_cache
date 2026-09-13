# TR-036 Read-Only SQL Transactions

`BeginSQLTransactionWithOptions` accepts `ReadOnly: true` for analytical,
backup, and recovery clients that must not stage mutations accidentally.

```go
transaction, err := hatriecache.BeginSQLTransactionWithOptions(trie, hatriecache.SQLTransactionOptions{
	ReadOnly: true,
})
if err != nil {
	return err
}
defer transaction.Rollback()

result, err := transaction.Query(ctx, `FROM CACHE('people') SELECT name`, nil, hatriecache.SQLQueryOptions{})
if err != nil {
	return err
}
_ = result
```

`ReadOnly` defaults to `false`, so existing transactions are unchanged. A
read-only transaction still provides the normal private snapshot and allows
`Query`. `Execute` returns `ErrSQLTransactionReadOnly` before compiling or
running a mutation. `Commit` succeeds as a no-op because no writes can be
staged.

This is an in-process API guard. It does not change command wire formats,
journal records, snapshots, or persistence. It also adds no work to ordinary
queries or writable transactions.

## Verification

```text
make test-tr036-read-only-transaction
make race-tr036-read-only-transaction
make vet-tr036-read-only-transaction
make benchmark-tr036-read-only-transaction
```

The five benchmark samples for the rejected mutation guard were:

```text
14.80 ns/op  0 B/op  0 allocs/op
13.23 ns/op  0 B/op  0 allocs/op
14.65 ns/op  0 B/op  0 allocs/op
15.31 ns/op  0 B/op  0 allocs/op
13.80 ns/op  0 B/op  0 allocs/op
```

The guard is allocation-free. This benchmark measures only an attempted write
against a read-only transaction; it is not a claim that read-only mode makes
queries faster.
