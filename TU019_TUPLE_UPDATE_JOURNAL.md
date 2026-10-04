# T-U19 Tuple Update Journal

`hatDataStructure.TupleFieldUpdateJournalRecord` is a bounded, replayable record
for durable tuple field updates. It packages a sequence number, logical key,
and the existing atomic tuple operations into an HTJ1 binary frame.

## Example

```go
record := hatDataStructure.TupleFieldUpdateJournalRecord{
	Sequence: 42,
	Key:      "orders/42",
	Updates: []hatDataStructure.TupleFieldUpdate{
		{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("paid")},
		{Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 1},
		{Index: 2, Kind: hatDataStructure.TupleFieldSplice, Start: 2, Remove: 1, Insert: []byte("X")},
	},
}

frame, err := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(record)
if err != nil {
	return err
}

decoded, err := hatDataStructure.UnmarshalTupleFieldUpdateJournalRecord(frame)
if err != nil {
	return err
}

updated, err := decoded.Apply(tuple)
```

For streaming storage, `WriteTupleFieldUpdateJournalRecord` appends one frame
to an `io.Writer`, and `ReadTupleFieldUpdateJournalRecord` reads exactly one
frame from an `io.Reader`. Decoding owns its variable-length byte slices, so a
caller may reuse its input buffer after the call.

## HTJ1 format

Each frame contains a four-byte `HTJ1` magic, version byte, little-endian
uint32 payload length, bounded payload, and CRC32C over the version, length, and
payload. The payload uses unsigned varints for lengths and indexes, zig-zag
varints for signed deltas, and one operation tag per update.

The decoder rejects truncated frames, invalid CRCs, unknown versions or update
operations, duplicate field indexes, oversized records, and invalid splice or
integer-update arguments. The current limits are 4 MiB per frame, 32 KiB per
key, and 256 updates per record.

The CRC detects accidental corruption; it is not an authentication mechanism.
Applications that need tamper resistance must add authenticated encryption or
an authenticated transport. The package does not fsync, rotate files, elect a
leader, or decide replay policy.

## Verification

```text
make test-tu19
make verify-tu19
make benchmark-tu19
```

The caller remains responsible for append durability, sequence validation,
deduplication, and routing the record to the tuple owner before calling
`Apply`.
