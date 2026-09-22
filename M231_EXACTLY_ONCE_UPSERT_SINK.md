# M231: Exactly-Once Upsert Sink Identities

## Contract

`ExactlyOnceUpsertSink` is a small sink-side state machine for ordered upserts
and tombstones. Each record carries a source `Sequence`, a stable destination
`OutputID`, a key, and a value:

```go
sink, err := hatReplication.NewExactlyOnceUpsertSink("orders")
decision, err := sink.Begin(hatReplication.ExactlyOnceUpsertSinkRecord{
	Sequence: 1,
	OutputID: "order-1",
	Key:      []byte("1"),
	Value:    []byte("paid"),
})
if err != nil {
	// Handle a gap, conflict, or pending record.
}
if decision.Action == hatReplication.ExactlyOnceUpsertSinkApply || decision.Action == hatReplication.ExactlyOnceUpsertSinkResume {
	// Apply the upsert or tombstone to the external destination.
	_, err = sink.Commit("order-1")
}
```

`Begin` returns:

- `Apply` for the next contiguous source sequence.
- `Resume` when the same pending record is retried after a restart.
- `Skip` when a committed sequence is delivered again.

Gaps, conflicting same-sequence identities, malformed identities, and
oversized payloads fail closed. `Abort` clears a pending record for retry.
`OutputID` is the stable identity of the destination row/object and may be
reused by later updates to that target.

## Durability boundary

The sink does not call external callbacks and cannot atomically commit an
arbitrary database, queue, or HTTP endpoint. The embedding service must write
the upsert and the returned sink snapshot atomically, or make the destination
operation idempotent by `OutputID`. The `uos1` snapshot stores the committed
sequence and last output identity plus the full pending record needed to resume
an interrupted write.

```go
snapshot, err := sink.Commit(record.OutputID)
encoded, err := snapshot.MarshalBinary()
// Persist encoded together with the external upsert in one transaction.
```

The first source sequence is `1`. `RestartSequence` returns the first sequence
not committed and reports explicit exhaustion instead of returning a valid
looking zero sequence.

## Wire/storage measurement

Machine: AMD Ryzen 9 5950X, Linux amd64. Command:
`make benchmark-m231-sink` (`go test ... -benchmem -count=5`). The JSON control
uses the same snapshot structure and payload.

| Operation | Binary median | JSON median | Improvement | Binary memory |
| --- | ---: | ---: | ---: | ---: |
| Begin plus Commit | 401.9 ns/op, 208 B/op, 8 allocs | n/a | state-machine cost | 208 B/op, 8 allocs |
| Snapshot marshal | 212.1 ns/op, 80 B/op, 1 alloc | 574.0 ns/op, 256 B/op, 2 allocs | 2.71x CPU, 3.20x lower B/op, 2x fewer allocs | 80 B/op |
| Snapshot unmarshal | 259.9 ns/op, 120 B/op, 6 allocs | 3,329 ns/op, 448 B/op, 13 allocs | 12.81x CPU, 3.73x lower B/op, 2.17x fewer allocs | 120 B/op |

The state machine intentionally copies bounded key/value payloads for restart
safety; that is the main cost of `Begin`. The compact snapshot path is the
storage and transfer optimization, not a claim that external side effects
become transactional automatically.

## Verification

- `make test-m231-sink`: apply, resume, skip, abort, gap, conflict, bounds, and malformed-snapshot tests.
- `make test-m231-sink-package`: complete `hat/hatReplication` package tests.
- `make race-m231-sink`: focused race test.
- `make vet-m231-sink`: package vet.
