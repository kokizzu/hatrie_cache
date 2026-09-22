# M233 Sink Retry Outbox

## Status

Adopted as an opt-in `hatReplication.SinkRetryQueue`. The feature combines a
bounded outbox, stable output-identity deduplication, deterministic retry
backoff, and compact restart state for disconnected sink connections.

The implementation is inspired by Tarantool queue task identities and retry
metadata, Materialize's durable output progress, and ClickHouse-style bounded
background work. It is deliberately a library primitive: it does not open
connections, perform network I/O, or claim that an external write is durable.

## Usage

```go
queue, err := hatReplication.NewSinkRetryQueue(hatReplication.SinkRetryOptions{
    Source: "orders",
})
if err != nil {
    return err
}

_, err = queue.Enqueue(record, time.Now())
if err != nil {
    return err
}

delivery, ok, err := queue.Next(time.Now())
if err != nil {
    return err
}
if ok {
    if err := sendToOutput(delivery.Record); err != nil {
        return queue.Retry(delivery.Record.OutputID, delivery.Record.Sequence, time.Now())
    }
    return queue.Ack(delivery.Record.OutputID, delivery.Record.Sequence)
}
```

`record.OutputID` is the deduplication key. An identical record is reported as
`SinkRetryDuplicate` without increasing memory. A newer sequence for the same
output replaces an older pending record and resets its attempt schedule. An
in-flight record is never replaced; the caller must finish or retry it first.
Older sequences and conflicting payloads are rejected.

## Bounds And Defaults

The default queue accepts 1,024 pending records or 64 MiB of record data,
whichever is reached first. The default retry delay is 100 ms with exponential
backoff capped at 30 s. The limits and delays are configurable per queue, with
hard safety caps of 1,048,576 records and 1 GiB.

`Next(now)` takes an explicit timestamp, so tests and schedulers do not depend
on hidden wall-clock state. Due work is selected by a min-heap; stale heap
entries from an upsert replacement are ignored by generation token rather than
requiring a linear heap scan.

## Restart And Durability

`SinkRetryQueueSnapshot.MarshalBinary` is the compact SRT1 storage format. It
contains the source, output records, attempt counts, due timestamps, and the
in-flight marker. `NewSinkRetryQueueFromBinary` validates source identity,
record bounds, duplicate identities, and configured queue limits before
installing state.

An item that was in flight when the snapshot was taken is restored without an
owner and becomes immediately claimable. This prevents a process crash from
stranding work. The embedding service must atomically persist the snapshot with
the durability boundary required by its output system. JSON remains available
as a compatibility fallback by marshaling `SinkRetryQueueSnapshot` with the
standard `encoding/json` package; SRT1 is the measured compact path.

The queue should normally be combined with `ExactlyOnceUpsertSink` and
`SinkProgressEnvelope`: the sink controls ordered/idempotent application, the
retry queue controls bounded disconnected work, and the progress envelope
couples acknowledged output with source frontier advancement.

## Measurement

Five benchmark samples were run on Linux amd64, AMD Ryzen 9 5950X. The codec
comparison used the same 128-record snapshot for both SRT1 and JSON.

| Operation | SRT1 / queue median | JSON median | SRT1 B/op | JSON B/op | SRT1 allocs/op | JSON allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Enqueue + claim + ack | 396.1 ns/op | n/a | 238 | n/a | 6 | n/a | bounded queue path |
| Claim + retry + reclaim + ack | 692.8 ns/op | n/a | 288 | n/a | 9 | n/a | bounded retry path |
| Snapshot marshal | 21,030 ns/op | 46,385 ns/op | 14,760 | 20,590 | 4 | 2 | 2.21x CPU, 28.3% lower bytes, 2x more allocs |
| Snapshot unmarshal | 27,086 ns/op | 293,168 ns/op | 23,664 | 36,464 | 389 | 402 | 10.82x CPU, 35.1% lower bytes, 3.3% fewer allocs |

The binary path trades two additional marshal allocations for materially lower
CPU, wire/storage size, and unmarshal allocations. The queue's map and heap
are intentionally bounded; a caller that needs durable dead-letter retention
or cross-process ownership must add that policy outside this package.

## Verification

- `make test-m233-sink-retry`
- `make test-m233-sink-retry-package`
- `make race-m233-sink-retry`
- `make vet-m233-sink-retry`
- `make benchmark-m233-sink-retry`
