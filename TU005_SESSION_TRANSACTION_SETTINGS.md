# T-U05 Session Transaction Settings

`hatCache.SQLTransactionSession` is an opt-in policy wrapper around the
existing SQL transaction engine. It lets one client/session keep validated
defaults for isolation, read-only mode, and timeout without changing the
backward-compatible package-level transaction functions.

## Example

```go
trie := hatCache.CreateHatTrie()
defer trie.Destroy()

session, err := hatCache.NewSQLTransactionSession(trie)
if err != nil {
	return err
}
if err := session.SetOptions(hatCache.SQLTransactionOptions{
	Isolation: hatCache.SQLTransactionIsolationSerializable,
	ReadOnly:  true,
	Timeout:   2 * time.Second,
}); err != nil {
	return err
}

transaction, err := session.Begin()
if err != nil {
	return err
}
defer transaction.Rollback()
```

Each `Begin` copies the defaults current at its start. Later `SetOptions` calls
affect later transactions only; an existing transaction keeps its isolation,
read-only, and deadline settings. `ResetOptions` restores the zero-value
defaults.

## Defaults And Boundaries

The default session is snapshot-isolated, writable, and has no timeout. The
feature is opt-in and does not alter `BeginSQLTransaction` or
`BeginSQLTransactionWithOptions`.

Durability is deliberately not exposed as a session setting: the current
transaction implementation provides snapshot/commit behavior but does not
promise a durable commit protocol. Authorization is also caller-owned; a
session groups transaction policy but does not identify a principal or grant
permissions.

`SetOptions` validates isolation and timeout before publishing the new value.
Concurrent option reads and updates are serialized; a transaction receives one
consistent option copy.

## Verification And Measurement

Run the focused checks with:

```text
make test-t-u05
make test-t-u05-package
make race-t-u05
make vet-t-u05
make benchmark-t-u05
```

Five `-benchmem` samples ran on an AMD Ryzen 9 5950X host. The direct option
copy is a control for the policy read itself; it is not a transaction begin
benchmark.

| Path | Median | Bytes/op | Allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Direct option copy control | 0.251 ns/op | 0 | 0 | baseline |
| `SQLTransactionSession.Options` | 4.794 ns/op | 0 | 0 | 19.1x slower; +4.543 ns |

Raw post-implementation samples:

```text
Direct: 0.2559, 0.2481, 0.2514, 0.2525, 0.2469 ns/op; 0 B/op; 0 allocs/op
Session: 4.780, 4.787, 4.794, 4.854, 4.799 ns/op; 0 B/op; 0 allocs/op
```

The overhead buys a stable, validated per-session policy boundary and is paid
only when reading the defaults. Transaction snapshot capture and execution are
not changed.
