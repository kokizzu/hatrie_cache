# SQL Sink Commit Coordination

`hatSql.SQLSinkCommitCoordinator` is an opt-in commit gate for sinks that may
receive the same transaction more than once. It deduplicates a `(sink,
transaction_id)` pair, single-flights concurrent attempts, and stores the
acknowledged sink progress needed to reconstruct the decision after restart.

```go
coordinator := hatSql.NewSQLSinkCommitCoordinator()
commit := hatSql.SQLSinkCommit{
    Sink:          "warehouse",
    TransactionID: "source-42",
    Progress: []hatSql.SQLSinkProgress{
        {Sink: "warehouse", Partition: "eu", Frontier: 120},
    },
}

committed, err := coordinator.Commit(commit, func() error {
    return writeSinkTransaction(commit)
})
// committed is true only for the callback that admitted the transaction.
// A successful duplicate returns false, nil and does not call the callback.
```

## Contract

- `Sink` and `TransactionID` must be non-empty after trimming.
- `Progress` must contain at least one entry. Every entry must name the same
  sink and a distinct partition.
- The first valid attempt runs `apply`. A successful retry is suppressed.
- Reusing a transaction ID with different progress returns
  `ErrSQLSinkCommitConflict`; it never calls the callback.
- A callback error is not retained, so a later attempt may retry it.
- A callback panic is cleaned up before the original panic is re-raised. Calls
  waiting on that callback receive `ErrSQLSinkCommitApplyPanic` and a later
  attempt may retry it.
- `Snapshot` returns only committed entries in deterministic order. `Restore`
  validates the complete input before replacing the current state and rejects
  restore while a callback is in flight.

The coordinator stores transaction metadata, not sink payloads. It does not
perform a network write, transaction, or two-phase commit itself.

## Durability Boundary

This is an idempotent commit gate, not a universal exactly-once guarantee.
Persist `Snapshot()` together with the sink's durable transaction state, or
make the sink write and the commit record one atomic transaction. If the sink
side effect succeeds and the process crashes before its commit record is
durable, a retry can run the callback again. The sink callback must therefore
be transactional with the record or be idempotent for the transaction ID.

Callers also own retention and compaction of old commit records. A coordinator
that retains every transaction indefinitely will grow with the number of
commits; prune only after the sink's retry/recovery window has passed.

## Measured Cost

Measured with `go test ./hat/hatSql -run '^$' -bench
'BenchmarkSQLSinkCommitCoordinator' -benchmem -count=5` on Linux/amd64 with an
AMD Ryzen 9 5950X:

| Path | Time | Heap | Allocs |
| --- | ---: | ---: | ---: |
| New committed transaction | 0.64-0.72 us/op | 375-409 B/op | 5/op |
| Successful duplicate | 0.154-0.167 us/op | 72 B/op | 2/op |

The duplicate path is the intended retry win. New commits pay metadata
validation and retention cost, so this API should be enabled where duplicate
delivery is a real concern rather than inserted into every sink write path.
