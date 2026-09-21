# TT-007 Snapshot-plus-WAL Join

`hatDataStructure.TupleFieldOperationJournal` now supports an opt-in join
lease for snapshot-based recovery:

```go
join, err := journal.BeginSnapshotJoin(snapshotSequence)
if err != nil {
	return err
}
defer join.Close()

delta, err := join.Replay(snapshotSequence, 0)
if err != nil {
	return err
}
_, err = hatDataStructure.ReplayTupleFieldOperationRecords(snapshotTuple, delta)
```

The caller must create a consistent data snapshot at `snapshotSequence` and
associate that sequence with the snapshot metadata. The join lease then keeps
the journal records after that sequence available while the snapshot is being
transferred or restored. If retaining the required records would exceed the
journal's configured record or byte limit, append returns
`ErrTupleFieldOperationJournalLimit` instead of silently compacting away the
join delta. Closing the lease resumes normal bounded compaction.

Multiple joins are supported. The oldest active snapshot sequence determines
the retention fence. A join is idempotently closable, rejects replay before its
snapshot sequence, and rejects replay after close. The normal journal path
does not allocate join state or scan a lease map until the first join starts.

This is the retention and replay part of TT-007. Snapshot encoding, transport,
node authentication, and automatic store-wide bootstrap remain caller-owned;
the journal API provides the correctness boundary those layers need.

## Operational guidance

Use a bounded timeout around snapshot transfer and always close the join in a
`defer`. Monitor `ErrTupleFieldOperationJournalLimit` as backpressure: either
finish or cancel the transfer, increase the configured journal bound, or retry
from a newer snapshot. Do not treat the error as permission to discard the
join's delta.

The journal validates operation IDs, update payloads, sequence coordinates,
checksums, and configured byte/record bounds as before. Join sequence numbers
are metadata only and do not bypass those validation or persistence checks.
