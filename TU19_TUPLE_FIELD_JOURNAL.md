# T-U19 Tuple Field-Operation Journal

T-U19 adds a bounded, deterministic journal record for the existing atomic tuple
field operations in `hat/hatDataStructure`. It makes `Set`, `Splice`, and
`AddInt64` batches replayable without changing the direct in-memory update API.

## API

```go
record := hatDataStructure.TupleFieldUpdateJournalRecord{
    Sequence:     42,
    FormatVersion: 7,
    Updates: []hatDataStructure.TupleFieldUpdate{
        {Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("after")},
        {Index: 1, Kind: hatDataStructure.TupleFieldSplice, Start: 1, Remove: 2, Insert: []byte("XYZ")},
        {Index: 2, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 2},
    },
}

wire, err := hatDataStructure.EncodeTupleFieldUpdateJournalRecord(record)
decoded, err := hatDataStructure.DecodeTupleFieldUpdateJournalRecord(wire)
updated, err := hatDataStructure.ApplyTupleFieldUpdateJournalRecord(cache, decoded, 7)
```

`ApplyTupleFieldUpdateJournalRecord` checks `FormatVersion` before applying the
batch. The underlying `TupleFieldOffsetCache.ApplyUpdates` validation remains
atomic: an invalid field operation returns an error and leaves the input cache
unchanged.

## HUF1 format

The encoding is a fixed-width binary record with a CRC32C checksum:

| Part | Size | Contents |
| --- | ---: | --- |
| Magic | 4 bytes | `HUF1` |
| Wire version | 1 byte | `1` |
| Sequence | 8 bytes | Big-endian monotonic journal sequence supplied by the caller |
| Format version | 8 bytes | Big-endian tuple/schema format fence supplied by the caller |
| Update count | 4 bytes | Big-endian number of field operations |
| Update entries | 25 bytes plus payload each | Index, operation kind, splice fields, delta, and payload length |
| Payload | Variable | `Set.Value` or `Splice.Insert` bytes |
| Checksum | 4 bytes | CRC32C Castagnoli over all preceding bytes |

The decoder rejects bad magic, unsupported wire versions, truncated or trailing
bytes, checksum failures, duplicate indexes, unsupported operation kinds, and
invalid operation fields. Decoded payloads are copied, so the returned record
does not alias the input wire buffer.

The codec enforces these limits:

| Limit | Value |
| --- | ---: |
| Updates per record | 256 |
| Set and splice payload bytes | 1 MiB |
| Complete encoded record | 2 MiB |

The package does not choose a storage location, append policy, fsync policy, or
transaction boundary. A storage layer can append the encoded bytes to its WAL,
persist the record, and replay it with the expected format version. The bounded
record and checksum keep that integration surface explicit.

## Benchmark

The benchmark was run through `make codex-tu19-field-journal-bench` with five
runs per benchmark on Linux amd64, AMD Ryzen 9 5950X 16-Core Processor. The
direct benchmark is the pre-journal in-memory apply baseline; replay includes
journal validation plus the same atomic tuple update.

| Path | Median ns/op | B/op | allocs/op | Relative to direct apply |
| --- | ---: | ---: | ---: | ---: |
| Direct apply baseline | 71.5 | 16 | 1 | 1.00x |
| Journal encode | 127.5 | 112 | 1 | 1.78x |
| Journal decode | 199.2 | 304 | 3 | 2.79x |
| Journal replay | 103.4 | 16 | 1 | 1.45x |

The journal is intentionally not a faster hot path than direct mutation. Its
benefit is a bounded, checksummed, version-fenced record that can be persisted
and replayed for recovery. Applications that already have a durable WAL can
use the codec at the durability boundary and keep direct apply for ordinary
in-memory work.

### Raw benchmark runs

```text
BenchmarkTU19DirectApply: 71.14 73.01 71.51 73.62 70.59 ns/op; 16 B/op; 1 alloc/op
BenchmarkTU19JournalEncode: 125.5 127.5 126.4 129.3 133.5 ns/op; 112 B/op; 1 alloc/op
BenchmarkTU19JournalDecode: 208.1 199.2 200.5 191.5 195.6 ns/op; 304 B/op; 3 alloc/op
BenchmarkTU19JournalReplay: 103.0 101.5 103.4 106.2 108.6 ns/op; 16 B/op; 1 alloc/op
```

The earlier baseline-only run measured 111.2 ns/op median for direct apply on a
different run. The comparison table above uses the same five-run invocation for
all four paths so the relative figures are internally comparable.
