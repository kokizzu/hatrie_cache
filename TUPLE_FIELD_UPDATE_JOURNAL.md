# Tuple Field-Update Journal

`hatDataStructure` exposes an opt-in binary record for durable replay of one
tuple field-operation batch. It packages the existing atomic
`TupleFieldOffsetCache.ApplyUpdates` operations with a sequence, key, version,
and checksum. It does not change the command journal or any default wire and
storage format.

## API

```go
record := hatDataStructure.TupleFieldUpdateJournalRecord{
	Sequence: 42,
	Key:      "orders/42",
	Updates: []hatDataStructure.TupleFieldUpdate{
		{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("east")},
		{Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 8},
		{Index: 2, Kind: hatDataStructure.TupleFieldSplice, Start: 2, Remove: 2, Insert: []byte("XYZ")},
	},
}

encoded, err := hatDataStructure.MarshalTupleFieldUpdateJournal(record)
decoded, err := hatDataStructure.UnmarshalTupleFieldUpdateJournal(encoded)
updated, err := tupleCache.ApplyUpdates(decoded.Updates)
```

The decoder copies the key and byte fields, so the caller can reuse or release
the input buffer after decoding. The encoder is deterministic and does not
retain caller-owned buffers.

## Binary Format

The versioned `HTU1` frame is:

| Field | Encoding |
| --- | --- |
| Magic | Four ASCII bytes, `HTU1` |
| Version | One byte, currently `1` |
| Sequence, lengths, indexes | Canonical unsigned varints |
| Key | Length-prefixed bytes |
| Update count | Canonical unsigned varint |
| `SET` | Field index, kind, length-prefixed value |
| `SPLICE` | Field index, kind, start, remove, length-prefixed insert bytes |
| `ADD_INT64` | Field index, kind, signed delta as eight big-endian bytes |
| Checksum | CRC32C Castagnoli over every preceding byte, little-endian |

The decoder rejects unknown versions and kinds, non-canonical varints,
truncated or trailing payloads, checksum failures, negative values represented
through the public API, and records outside the bounded limits. The current
limits are a 1 MiB key, 1,024 operations, and a 16 MiB complete encoded
record. `Sequence`, `Key`, and `Updates` must be non-empty/positive.

## Replay Contract

1. Append the encoded bytes to the application's durable journal.
2. On recovery, decode and verify the record before applying it.
3. Apply `decoded.Updates` with `TupleFieldOffsetCache.ApplyUpdates`.
4. Advance the caller's journal sequence only after the atomic update succeeds.

The codec cannot infer tuple field count or types from the journal record. The
existing update application path remains responsible for field indexes,
duplicate operations, splice ranges, integer type checks, and overflow
handling.

## Benchmark

Representative record: one `SET`, one `ADD_INT64`, and one `SPLICE`, key
`orders/42`, measured on Linux/amd64 with an AMD Ryzen 9 5950X using five
benchmark repetitions. JSON is the equivalent `encoding/json` envelope and
is a comparison baseline, not a changed default.

| Operation | Binary median | JSON median | Improvement | Binary wire | JSON wire |
| --- | ---: | ---: | ---: | ---: | ---: |
| Marshal | 91.03 ns/op, 48 B/op, 1 alloc | 763.3 ns/op, 368 B/op, 2 allocs | 8.39x faster; 7.67x fewer bytes/op; 2x fewer allocs | 46 B | 290 B |
| Unmarshal | 202.5 ns/op, 336 B/op, 5 allocs | 4,655 ns/op, 992 B/op, 14 allocs | 22.99x faster; 2.95x fewer bytes/op; 2.8x fewer allocs | 46 B input | 290 B input |

The binary record is 6.30x smaller on the wire for this workload. These are
codec measurements only; filesystem sync, compression, encryption, and the
cost of applying the tuple mutation are outside this benchmark.
