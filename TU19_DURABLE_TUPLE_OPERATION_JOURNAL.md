# T-U19 Durable Tuple Field-Operation Journal

This is a Tarantool-inspired, opt-in journal for the existing atomic tuple
field operations in `hatDataStructure`. It records `SET`, byte `SPLICE`, and
big-endian `int64 ADD` batches as bounded, versioned, CRC32C-protected records.

The journal is intentionally separate from the default tuple and cache paths.
Callers associate one journal with a tuple or tuple namespace, append an
operation before publishing the corresponding state, and replay retained
records after a snapshot or restart.

## Example

```go
journal, err := hatDataStructure.OpenTupleFieldOperationJournal(
    "data/orders.tuple-journal",
    hatDataStructure.TupleFieldOperationJournalOptions{},
)
if err != nil {
    return err
}

record, err := journal.Append(hatDataStructure.TupleFieldOperation{
    OperationID: "client-request-42",
    Updates: []hatDataStructure.TupleFieldUpdate{
        {Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 1},
        {Index: 2, Kind: hatDataStructure.TupleFieldSplice, Start: 0, Remove: 0, Insert: []byte("ok")},
    },
})
if err != nil {
    return err
}

records, err := journal.Replay(lastSnapshotSequence, 0)
if err != nil {
    return err
}
tuple, err = hatDataStructure.ReplayTupleFieldOperationRecords(tuple, records)
_ = record.Sequence
```

## Guarantees

- Empty options retain 256 records and an 8 MiB encoded journal; hard limits
  are 65,536 records and 64 MiB.
- Operation IDs are bounded and idempotent while their record is retained.
  Reusing an ID with different updates returns a conflict error.
- Invalid updates are rejected before the journal state changes.
- When the record or byte bound is reached, the oldest records are compacted
  and `Replay` reports a history gap for callers that start too far behind.
- `UnmarshalBinary` validates the `TJF1` version, lengths, sequence continuity,
  duplicate field indexes, and CRC32C before replacing state.
- File-backed appends write a `0600` temporary file, sync it, atomically rename
  it, and sync the parent directory before publishing the new in-memory state.
- Replay returns the original tuple unchanged if any operation fails due to a
  field type, range, or arithmetic-overflow error.

The journal stores operations, not tuple identity or application-level
transactions. The caller owns tuple association, snapshot coordination,
replication, and recovery policy.

## Measurements

AMD Ryzen 9 5950X, Linux/amd64, five `-benchmem` samples:

| Path | Median | Memory | Interpretation |
| --- | ---: | ---: | --- |
| Existing direct `ApplyUpdates` baseline | 216.3 ns/op | 40 B, 2 allocs | Tuple mutation only |
| In-memory `Append` | 1,514 ns/op | 718 B, 8 allocs | Bounded record ownership and deduplication |
| `MarshalBinary` | 251.0 ns/op | 192 B, 2 allocs | Explicit wire/storage encoding and CRC |
| Replay plus tuple apply | 411.9 ns/op | 344 B, 5 allocs | One-record recovery path |
| File-backed durable `Append` | 1.638 ms/op | 6,390 B, 29 allocs | Temporary file, rename, file sync, directory sync |

The journal is not a faster replacement for direct tuple mutation. It is an
opt-in recovery boundary. The default tuple path has no journal lookup or
allocation; callers should use the in-memory form when they need bounded
replayable history and the path-backed form only when per-operation durability
is worth the filesystem sync cost.
