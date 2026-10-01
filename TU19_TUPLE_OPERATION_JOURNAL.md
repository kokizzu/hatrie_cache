# T-U19: Durable Tuple Field-Operation Journal

`hatDataStructure.TupleFieldOperationJournal` is an opt-in durable journal for
atomic tuple field updates. It records the existing allocation-light
`TupleFieldSet`, `TupleFieldSplice`, and `TupleFieldAddInt64` operations instead
of serializing a complete replacement tuple for every update.

## API

```go
journal, err := hatDataStructure.OpenTupleFieldOperationJournal(
    "data/tuple-operations.journal",
    initialTuple,
    hatDataStructure.TupleFieldOperationJournalOptions{
        SchemaVersion: 7,
    },
)
if err != nil {
    return err
}
defer journal.Close()

record, err := journal.Apply([]hatDataStructure.TupleFieldUpdate{{
    Index: 0,
    Kind:  hatDataStructure.TupleFieldSet,
    Value: []byte("east"),
}})
```

`OpenTupleFieldOperationJournal` uses the caller-supplied tuple as the replay
base and replays every complete journal record. `Snapshot` returns a clone of
the current tuple. `Sequence` reports the last committed sequence, and
`Close` prevents further writes or snapshots.

`TupleFieldAddInt64` requires exactly eight bytes in big-endian signed-int64
representation and rejects overflow. A rejected batch is validated before
allocation, does not append a frame, and does not advance the sequence.

## Durability and recovery

- The default is one `fsync` per committed record.
- `UnsafeNoSync: true` explicitly skips that sync for workloads that can
  tolerate loss of recently written records.
- The default maximum encoded frame is 1 MiB. `MaxRecordBytes` may lower it,
  with a bounded implementation maximum of 16 MiB.
- Every frame has a contiguous sequence, schema version, field count, payload
  length, and CRC-32/IEEE checksum. A short header, short payload, checksum
  mismatch, schema mismatch, field-count mismatch, or sequence gap fails
  recovery rather than silently replaying partial data.
- The journal file is created with mode `0600`.

The journal is not a consensus log and does not replace a complete database
snapshot/WAL protocol. The caller owns the base tuple, snapshot rotation, and
coordination with any other indexes or replicated state. Keep a clean base
snapshot with the journal when using it for recovery.

## Frame format

Each record is an `HTJ1` frame with a fixed 36-byte big-endian header followed
by the encoded update payload:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | Magic `HTJ1` |
| 4 | 1 | Frame version |
| 5 | 1 | Flags, currently zero |
| 6 | 2 | Header size, currently 36 |
| 8 | 8 | Sequence |
| 16 | 8 | Schema version |
| 24 | 4 | Tuple field count |
| 28 | 4 | Payload length |
| 32 | 4 | CRC-32/IEEE of payload |

The payload starts with a `uint32` update count. Each update stores its field
index and kind, followed by only the bytes needed by that kind. Set and splice
operations use length-prefixed byte fields; add operations store an int64
delta. The decoder rejects unknown kinds, invalid indexes, duplicate fields,
integer overflow, and trailing payload bytes.

## Benchmark

Command: `make benchmark-round38-tuple-journal` (`-benchtime=10x -count=5`),
AMD Ryzen 9 5950X, Linux/amd64. Values below show the five raw samples and
the median. The memory-only, unsafe, and fsync cases use the same two-field
fixed-width set operation. Temporary benchmark files are created under
`testing.B.TempDir` and removed by the test framework.

| Case | Raw ns/op samples | Median | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Memory-only `ApplyUpdates` | 810, 633.1, 594.1, 637, 680 | 637 ns | 16 | 1 |
| Journal, `UnsafeNoSync` | 3491, 3088, 3993, 3018, 3156 | 3156 ns | 104 | 3 |
| Journal, default fsync | 1137479, 1153483, 1036780, 2282185, 1005099 | 1.137 ms | 104 | 3 |

Relative to the same-shape memory-only update, unsafe journaling costs about
5.0x median latency, +88 B/op, and two extra allocations. Default fsync costs
about 1,786x median latency in this run, with a 1.005 ms to 2.282 ms sample
range on this storage device. That cost is the durability guarantee, so this
API remains opt-in and callers should batch updates or use
`UnsafeNoSync` only when its loss semantics are acceptable.

For context, the existing 512-field fixed-width tuple benchmark measured
4,134 to 4,246 ns/op, 4,096 B/op, and one allocation. It is not an apples-to-
apples comparison because it scans a much larger tuple; the same-shape cases
above are the relevant overhead measurement.
