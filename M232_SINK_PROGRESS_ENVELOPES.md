# M232: Sink Progress Envelopes

## Coupled output and progress

`SinkProgressEnvelope` carries output records and the source frontier they
justify in one immutable message. `SinkProgressEmitter` refuses to emit an
empty output batch, so a consumer cannot observe a standalone progress claim
from this API.

```go
emitter, err := hatReplication.NewSinkProgressEmitter("orders", 0)
envelope, err := emitter.Emit([]hatReplication.ExactlyOnceUpsertSinkRecord{
	{Sequence: 1, OutputID: "order-1", Key: []byte("1"), Value: []byte("paid")},
}, 1)
```

The frontier is monotone. Every record must be newer than the emitter's
previous frontier and no record may be newer than the envelope frontier. A
batch may contain filtered source output and therefore advance the frontier
past its last emitted record, but the caller is responsible for having
durably handled all filtered input before emitting that envelope. Regressions,
overlapping records, malformed records, and oversized binary frames fail
closed.

The envelope is a transport/storage value, not a remote commit protocol. The
embedding service must write the output batch and the envelope atomically, or
make downstream application idempotent.

## SPG1 encoding and measurement

Machine: AMD Ryzen 9 5950X, Linux amd64. Command:
`make benchmark-m232-sink-progress` (`go test ... -benchmem -count=5`). The
benchmark envelope contains 32 upsert records; JSON uses the same structure.

| Operation | SPG1 median | JSON median | Improvement | SPG1 memory |
| --- | ---: | ---: | ---: | ---: |
| Emit one record | 171.9 ns/op | n/a | emitter state-machine cost | 96 B/op, 3 allocs/op |
| Marshal 32 records | 2,571 ns/op | 6,492 ns/op | 2.52x CPU, 2.30x lower B/op, 2x fewer allocs | 1,024 B/op, 1 alloc |
| Unmarshal 32 records | 4,247 ns/op | 36,975 ns/op | 8.71x CPU, 1.91x lower B/op, 1.13x fewer allocs | 3,464 B/op, 98 allocs |

The envelope removes the protocol ambiguity of separate output and progress
messages. SPG1 is compact and fast, while unmarshal still allocates one owned
payload per record to prevent caller mutation.

## Verification

- `make test-m232-sink-progress`: coupling, filtered frontier, immutability, monotonicity, and malformed-frame tests.
- `make test-m232-sink-progress-package`: complete `hat/hatReplication` package tests.
- `make race-m232-sink-progress`: focused race test.
- `make vet-m232-sink-progress`: package vet.
