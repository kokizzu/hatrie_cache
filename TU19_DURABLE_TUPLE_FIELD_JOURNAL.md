# T-U19 Durable tuple field-operation journal records

T-U19 adds an importable, deterministic HTFJ1 binary record for the existing
`hatDataStructure.TupleFieldUpdate` operations. It makes `SET`, `SPLICE`, and
big-endian `ADD_INT64` batches suitable for durable storage or transport while
preserving the existing atomic replay validator.

```go
record := hatDataStructure.TupleFieldUpdateJournalRecord{
	Sequence: 42,
	Key:      "customer-000042",
	Updates: []hatDataStructure.TupleFieldUpdate{
		{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("ready")},
		{Index: 2, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 7},
	},
}
payload, err := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(record)
decoded, err := hatDataStructure.UnmarshalTupleFieldUpdateJournalRecord(payload)
updated, err := decoded.ApplyTo(tuple)
```

## Wire and safety contract

- The record starts with `HTFJ`, version `1`, canonical unsigned varints, and
  ends with CRC32C (Castagnoli).
- Decoding copies key and operation byte fields; the caller may reuse the wire
  buffer immediately.
- Sequence and key are required. One record has at most 64 operations, a key
  at most 1 MiB, and a complete encoded record at most 16 MiB.
- Duplicate field indexes, unknown operation kinds, negative splice bounds,
  malformed varints, unsupported versions, trailing bytes, and checksum
  failures are rejected before replay.
- `ApplyTo` delegates field range, type, overflow, and atomicity checks to the
  existing `TupleFieldOffsetCache.ApplyUpdates` implementation.
- The codec is opt-in and does not replace existing command-journal formats or
  automatically route SQL/Cache commands. Callers can store the HTFJ1 bytes as
  a journal payload and choose when to replay them.

## Measurement

Five `go test -benchmem` samples on Linux/amd64, AMD Ryzen 9 5950X. The
workload contains four operations (`SET`, `SPLICE`, `ADD_INT64`, `SET`) for a
single tuple. Lower is better.

| Operation | Before JSON median | HTFJ1 median | CPU improvement | Before wire | HTFJ1 wire | Wire reduction | Before B/op | HTFJ1 B/op | Before allocs | HTFJ1 allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Marshal | 520.2 ns | 104.6 ns | 4.97x faster | 393 B | 64 B | 6.14x smaller | 464 | 64 | 2 | 1 |
| Unmarshal | 1,262 ns | 299.8 ns | 4.21x faster | 393 B | 64 B | 6.14x smaller | 1,231 | 416 | 10 | 6 |

Raw samples (`ns/op / B/op / allocs/op / wire_bytes/op`):

```text
before JSON marshal:   527.7/464/2/393, 520.2/464/2/393, 520.3/464/2/393, 502.2/464/2/393, 468.9/464/2/393
after HTFJ1 marshal:   104.6/64/1/64, 104.5/64/1/64, 106.8/64/1/64, 105.0/64/1/64, 104.3/64/1/64
before JSON unmarshal: 1283/1231/10/393, 1256/1231/10/393, 1264/1231/10/393, 1239/1231/10/393, 1262/1231/10/393
after HTFJ1 unmarshal: 305.3/416/6/64, 302.5/416/6/64, 299.8/416/6/64, 296.8/416/6/64, 298.4/416/6/64
```

The JSON control was a benchmark representation, not a previously persisted
tuple-journal contract. Existing journal and command formats remain unchanged;
the new record is available where a caller needs compact durable tuple
operations.

Reproduce with:

```sh
make benchmark-tu19-before
make benchmark-tu19-tuple-journal
make test-tu19-tuple-journal
make race-tu19-tuple-journal
make vet-tu19-tuple-journal
```
