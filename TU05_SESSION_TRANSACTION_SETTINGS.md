# Session Transaction Settings

`hatCache.SQLTransactionSession` provides one reusable, validated defaults
profile for SQL transactions. The zero-value profile preserves the existing
snapshot, writable, no-timeout, in-memory behavior.

## Usage

```go
journal, err := hatCache.OpenCommandJournal("/var/lib/hatrie/commands.journal")
if err != nil {
    return err
}
defer journal.Close()

session, err := hatCache.NewSQLTransactionSession(hatCache.SQLTransactionOptions{
    Isolation:  hatCache.SQLTransactionIsolationSerializable,
    ReadOnly:   false,
    Timeout:    5 * time.Second,
    Durability: hatCache.SQLTransactionDurabilityJournal,
    Journal:    journal,
})
if err != nil {
    return err
}

transaction, err := session.Begin(trie)
if err != nil {
    return err
}
defer transaction.Rollback()

if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('k', 'v')"); err != nil {
    return err
}
return transaction.Commit()
```

`Begin` copies the current options. Updating or resetting the session affects
future transactions only; it cannot change an already-running transaction.
`SetOptions` validates isolation, timeout, durability, and journal pairing.
`Reset` returns to the backward-compatible zero-value defaults.

## Durability

`SQLTransactionDurabilityMemory` is the default and keeps the original commit
path. `SQLTransactionDurabilityJournal` is opt-in and requires a non-nil
`CommandJournal`; the transaction is committed through the existing atomic
public-batch journal path, including append, sync, and rollback on failure.
There is intentionally no silent asynchronous or unsafe durability mode.

The session does not grant authorization or own the journal lifecycle. The
caller must control who may select a journal, and must close the journal after
all sessions using it have stopped.

## Measurements

Measurements were run on Linux/amd64 with an AMD Ryzen 9 5950X using three
100-iteration samples (`make benchmark-tu05-session`). The snapshot operation
dominates transaction creation, so session defaults add no meaningful memory
cost; the journal durability cost is explicit and remains disabled by default.

| Benchmark | Median time | Memory | Allocs | Comparison |
| --- | ---: | ---: | ---: | --- |
| Direct transaction begin | 2.12 ms/op | 79.9 KB/op | 94 | baseline |
| Session transaction begin | 2.04 ms/op | 79.7 KB/op | 94 | within noise, same alloc count |
| Session `Options()` | 5.6 ns/op | 0 B/op | 0 | zero-allocation read |
| Memory transaction commit | 2.18 ms/op | 203.9 KB/op | 468 | default |
| Journal transaction commit | 3.14 ms/op | 205.3 KB/op | 478 | 1.44x time, +1.4 KB, +10 allocs |

Raw benchmark output is kept in the command output from
`make benchmark-tu05-session`; rerun it on the target hardware before using
these numbers for capacity planning.

## Verification

The focused tests cover settings inheritance and reset, invalid option
rejection, journal replay after a committed transaction, and the required
journal guard. Run the package tests and race detector with the repository's
normal `make test` and `make race` targets.
