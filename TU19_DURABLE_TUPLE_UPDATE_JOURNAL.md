# Durable Tuple Update Journal

`hatDataStructure.TupleFieldUpdateJournal` is an opt-in durable log for
versioned tuple field operations. It complements `TupleFieldOffsetCache` and
`VersionedTuple` without coupling the log to a storage engine or a particular
key layout.

## Use

```go
format, err := hatDataStructure.NewTupleFormat(7, []hatDataStructure.TupleFieldSpec{
    {Name: "name", Type: hatDataStructure.TupleFieldString},
    {Name: "count", Type: hatDataStructure.TupleFieldInt64},
})
if err != nil {
    return err
}

journal, err := hatDataStructure.OpenTupleFieldUpdateJournal("data/updates.journal")
if err != nil {
    return err
}
defer journal.Close()

sequence, err := journal.Append(format.Version(), []hatDataStructure.TupleFieldUpdate{
    {Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 1},
})
if err != nil {
    return err
}
_ = sequence
```

For durable group commits, use `AppendBatch`. The complete batch is validated
before writing and one `Sync` covers every record:

```go
sequences, err := journal.AppendBatch([]hatDataStructure.TupleFieldUpdateJournalAppend{
    {SchemaVersion: format.Version(), Updates: []hatDataStructure.TupleFieldUpdate{
        {Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("beta")},
    }},
    {SchemaVersion: format.Version(), Updates: []hatDataStructure.TupleFieldUpdate{
        {Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 1},
    }},
})
```

`ReplayInto` validates the tuple format and applies records atomically to a
working copy. If a schema mismatch, invalid operation, or overflow occurs, it
returns the original tuple and the caller must not advance its checkpoint.

## Durability

`OpenTupleFieldUpdateJournal` enables `SyncOnAppend` and is the safe default.
`OpenTupleFieldUpdateJournalWithOptions` can set `SyncOnAppend: false` for an
explicit buffered-write policy. Call `Sync` at the chosen group-commit point;
`Close` also syncs pending bytes once.

The journal creates parent directories with mode `0700` and files with mode
`0600`. A partial final frame is treated as a crash tail and truncated during
open. A complete frame with a bad checksum, invalid sequence, unsupported
version, or malformed operation is rejected.

## Wire Format

Each frame is bounded HTU1 binary data:

1. Four-byte magic, one-byte format version, and four-byte body length.
2. Sequence, schema version, update count, and compact operation payloads.
3. CRC32C over the version, length, and payload.

`MaxRecordBytes` defaults to `1 MiB` and cannot exceed `64 MiB`. The update
count is bounded at `65536`, and a batch's temporary encoded memory is bounded
at `64 MiB`. `MarshalTupleFieldUpdateJournalRecord` and
`UnmarshalTupleFieldUpdateJournalRecord` expose the same bounded frame for
transport or alternate storage.

## Benchmark

Command:

```text
make benchmark-chg23-tuple-journal
```

Representative result on AMD Ryzen 9 5950X, Go benchmark package:

| Path | ns/op | B/op | allocs/op | wire bytes/op |
| --- | ---: | ---: | ---: | ---: |
| HTU1 encode | 126.3 | 192 | 2 | 72 |
| JSON baseline | 625.8 | 240 | 2 | 192 |
| HTU1 decode | 292.7 | 440 | 6 | 72 |
| Buffered append | 5,562 | 336 | 3 | n/a |
| Sync per append | 12,994,965 | 336 | 3 | n/a |
| Sync per 16-record batch | 10,892,712 per batch | 2,768 per batch | 35 per batch | n/a |

The binary codec is about `4.95x` faster and `2.67x` smaller than the JSON
baseline in this workload. A 16-record durable batch is about `19.1x` faster
per record than syncing every record, at the cost of a larger crash window
between batch boundaries. The benchmark is a local filesystem measurement;
operator capacity planning must repeat it on the target storage.
