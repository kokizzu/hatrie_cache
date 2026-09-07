# Source Ingestion Coordination

`hatSql.SQLSourceIngestionCoordinator` is an opt-in idempotency gate for
source transactions. It validates that all offsets belong to the declared
source, single-flights concurrent attempts, suppresses successful duplicate
transactions, detects conflicting replays, and supports deterministic
snapshot/restore.

```go
coordinator := hatSql.NewSQLSourceIngestionCoordinator()
ingestion := hatSql.SQLSourceIngestion{
    Source: "events",
    Transaction: hatSql.SQLSourceTransaction{
        ID: "broker-txn-42",
        Offsets: []hatSql.SQLSourceOffset{
            {Source: "events", Partition: "0", Offset: 120},
            {Source: "events", Partition: "1", Offset: 90},
        },
    },
}

ingested, err := coordinator.Ingest(ingestion, func() error {
    return applySourceTransaction(ingestion.Transaction)
})
// Only the first successful attempt returns true. A duplicate returns false,
// nil and does not call the callback.
```

## Contract

- `Source`, transaction ID, and the offset group must be non-empty.
- Every offset must name the declared source and a distinct partition.
- Offsets are normalized by partition for conflict comparison and snapshots.
- A successful duplicate is suppressed. Reusing an ID with different offsets
  returns `ErrSQLSourceIngestionConflict` and never invokes the callback.
- Callback errors are not retained, so a later attempt can retry the
  transaction.
- A callback panic is cleaned up before the original panic is re-raised; a
  later attempt can retry it.
- `Snapshot` returns only committed transaction metadata. `Restore` validates
  the complete input before replacing state and rejects restore while a
  callback is in flight.

The coordinator does not retain source rows or advance a source broker by
itself. Persist its snapshot together with the applied transaction and source
offset checkpoint, or use a durable idempotency record in the same transaction
as the data write. Otherwise a crash after applying the rows but before the
checkpoint can cause a callback retry. The underlying write must therefore be
transactional with the checkpoint or idempotent for the transaction ID.

`SQLSourceOffsetTracker.AdvanceTransaction` remains useful for monotone,
atomic offset movement, but by itself it does not remember transaction IDs.
Use both pieces when a source can replay a transaction with the same offsets.

## Measured Cost

Measured with `go test ./hat/hatSql -run '^$' -bench
'BenchmarkSQLSourceIngestionCoordinator' -benchmem -count=5` on Linux/amd64
with an AMD Ryzen 9 5950X:

| Path | Time | Heap | Allocs |
| --- | ---: | ---: | ---: |
| New committed transaction | 0.68-0.80 us/op | 376-459 B/op | 5/op |
| Successful duplicate | 0.143-0.161 us/op | 72 B/op | 2/op |

New transactions retain metadata in the coordinator, so callers own
retention and compaction after their replay/recovery window has passed.
