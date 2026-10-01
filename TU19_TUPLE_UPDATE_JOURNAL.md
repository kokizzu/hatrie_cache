# T-U19 Durable Tuple Field-Operation Journal

`hatDataStructure.TupleFieldUpdateJournal` is an opt-in writer-backed journal
for the existing atomic tuple field operations. It gives `TupleFieldSet`,
`TupleFieldSplice`, and `TupleFieldAddInt64` a bounded, versioned, replayable
record format without changing the default tuple or command-journal paths.

## Record contract

Each `TupleFieldUpdateJournalRecord` contains:

- a monotonic sequence;
- an opaque `TupleID` chosen by the caller;
- a positive tuple schema version;
- one or more non-duplicate field updates.

The HTJ1 binary record is length-framed by the journal and ends with a CRC32C.
It uses bounded uvarints for indexes and lengths, a signed varint for integer
deltas, and copies decoded variable-length fields so replay callbacks do not
borrow storage buffers.

The default record limit is `64 KiB`, the hard limit is `1 MiB`, and one record
may contain at most `1024` updates. Oversized or malformed input is rejected
before an unbounded allocation. CRC32C detects accidental corruption; it is
not authentication or encryption, so the backing journal still needs the
caller's normal access control and encryption policy.

## Durable append and replay

```go
file, err := os.OpenFile("updates.htj", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
if err != nil {
    return err
}
defer file.Close()

journal, err := hatDataStructure.NewTupleFieldUpdateJournal(file, hatDataStructure.TupleFieldUpdateJournalOptions{})
if err != nil {
    return err
}
_, err = journal.Append(tupleID, format.Version(), []hatDataStructure.TupleFieldUpdate{
    {Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 1},
})
```

When the writer implements `Sync() error`, the default append path calls it
after each complete frame. `NoSync` allows batching; an explicit `Sync()` is
still available. A failed write or sync poisons that journal instance so a
possibly torn frame cannot be followed by more records.

`ReplayTupleFieldUpdateJournal` validates every frame, rejects a truncated
tail, enforces strictly increasing sequences, and skips records at or before
the supplied checkpoint. It invokes the callback only after the complete
record has passed bounds and checksum validation. The caller owns tuple-ID
routing and any durable checkpoint transaction.

`ApplyTupleFieldUpdateJournalRecord` checks the schema version and applies the
record through the existing copy-on-write `VersionedTuple` path. A rejected
schema, type, range, or overflow operation does not mutate the input tuple.

## Compatibility and recovery

The schema version in each record prevents replaying positional updates into a
different tuple layout. Sequence checkpoints are exclusive, so a consumer can
persist the last successfully applied sequence and resume from it. The strict
truncated-tail behavior makes crash recovery fail closed; an operator must
repair or replace an incomplete journal frame instead of silently applying a
partial mutation.

The primitive is deliberately not automatically wired into every table or
`CommandJournal`: enabling it is a storage and durability decision with
additional bytes and sync latency. Existing tuple updates and journal users
retain their behavior and allocation profile unless they opt in.

## Benchmark

Runs used five `-benchmem` samples on Linux/amd64, AMD Ryzen 9 5950X. The
baseline was captured before the journal implementation. The append rows show
the first implementation and the final single-allocation frame path.

| Path | Raw samples | Median | Bytes/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing `ApplyUpdates`, before | 1392, 1365, 1377, 1407, 1422 ns/op | 1392 ns/op | 1408 | 1 |
| Existing `ApplyUpdates`, after | 1416, 1388, 1386, 1389, 1397 ns/op | 1389 ns/op | 1408 | 1 |
| Journal append, first implementation | 149.2, 148.4, 151.4, 147.8, 148.8 ns/op | 148.8 ns/op | 91 | 2 |
| Journal append, final | 127.3, 127.2, 124.6, 124.2, 125.5 ns/op | 125.5 ns/op | 48 | 1 |
| Record marshal, final | 109.0, 106.9, 108.4, 105.5, 108.5 ns/op | 108.4 ns/op | 45 | 1 |
| Replay 100 records, first implementation | 29504, 29592, 29684, 29393, 29410 ns/op | 29504 ns/op | 34448 | 502 |
| Replay 100 records, final | 27140, 27332, 27101, 27534, 26740 ns/op | 27140 ns/op | 31280 | 403 |

The final append path is `1.19x` faster than the first implementation, uses
`47%` fewer bytes, and halves its allocations. Reusing the bounded temporary
replay frame is `1.09x` faster, uses `9%` fewer bytes, and removes `99`
allocations for this 100-record workload. These are journal costs, not a claim
that journaling is faster than an in-memory tuple mutation; the existing
mutation path remains unchanged and allocation-free by default.

Verification targets:

```text
make test-t-u19
make test-t-u19-package
make race-t-u19
make vet-t-u19
make benchmark-t-u19-baseline
make benchmark-t-u19
```
