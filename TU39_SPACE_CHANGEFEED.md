# T-U39 Named-Space Changefeed

`hatReplication.SpaceChangefeed` is an opt-in bounded change log for one
named space. It supplies ordered insert/update/delete events, a fixed schema
version, reconnect checkpoints, explicit compaction, and non-blocking
backpressure. It does not start a goroutine, open a listener, or change any
existing write path.

## Example

```go
feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{
	Space:         "orders/eu",
	SchemaVersion: 7,
	MaxEvents:     4096,
	MaxBytes:      4 << 20,
})
if err != nil {
	return err
}

checkpoint := feed.InitialCheckpoint()
published, err := feed.Publish(
	hatReplication.SpaceChangeInsert,
	[]byte("order-1"),
	[]byte("status=paid"),
)
if err != nil {
	return err
}

changes, next, err := feed.ReadAfter(checkpoint, 256)
if err != nil {
	return err
}
_ = changes
_ = published

// Persist next with ChangefeedCheckpoint.MarshalBinary after applying the
// returned events, then release retained events through the durable cursor.
if err := feed.CompactThrough(next); err != nil {
	return err
}
```

`Space()` and `SchemaVersion()` identify the immutable feed metadata. Metadata
is not duplicated in every event, keeping reconnect batches compact. `Key` and
`Value` are owned copies; changing either after `ReadAfter` cannot mutate the
retained feed.

## Defaults And Limits

- Empty `SchemaVersion`, `MaxEvents`, or `MaxBytes` selects version `1`, `4096`
  events, or `4 MiB` respectively.
- An event must have a non-empty key and a valid operation.
- `ErrSpaceChangefeedEventTooLarge` means one event exceeds `MaxBytes`.
- `ErrSpaceChangefeedFull` means the configured event or byte budget is full.
  Producers must retry after a consumer calls `CompactThrough`; unread events
  are never silently evicted.
- A checkpoint older than the retained boundary returns
  `ErrSpaceChangefeedCompacted` and requires a caller-owned snapshot/rebuild.
- A checkpoint from another space or ahead of the feed is rejected.
- `Stats()` reports retained events/bytes and sequence boundaries for operator
  metrics and admission decisions.

The feed uses a fixed ring of event slots and packs each event's key/value into
one allocation. Reads copy a whole batch into one owned payload buffer. The
ring's slot metadata is bounded by `MaxEvents`; payload retention is bounded by
`MaxBytes`.

## Verification

Focused checks:

```sh
make test-tu39-space-changefeed
make race-tu39-space-changefeed
make vet-tu39-space-changefeed
```

The contract covers option defaults, ordered events, insert/update/delete
operations, caller-buffer isolation, concurrent publishers, checkpoint source
validation, future checkpoints, compaction gaps, and count/byte backpressure.

## Benchmark

Five `-benchtime=100ms` samples on Linux/amd64, AMD Ryzen 9 5950X. The naive
baseline is a bounded append-and-drop slice that copies key and value
separately. The candidate uses the fixed ring and packed payloads.

| Workload | Naive median | SpaceChangefeed median | CPU result | Naive B/op | Candidate B/op | Memory result | Naive allocs/op | Candidate allocs/op | Allocation result |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| Publish | 106.4 ns | 43.61 ns | 2.44x faster | 329 | 48 | 6.85x lower | 2 | 1 | 2.00x fewer |
| Read 64 events | 3,831 ns | 2,131 ns | 1.80x faster | 7,448 | 7,576 | 1.02x higher | 130 | 3 | 43.3x fewer |

The read byte count is effectively neutral and is 1.7% higher in this fixture
because the public event batch owns one contiguous payload buffer; the large
win is avoiding 128 per-event payload allocations. The retained feed itself is
bounded by the configured ring and byte budgets, unlike the naive slice whose
discarded backing entries retain payload pointers until its backing array is
replaced.

Raw samples were produced by:

```sh
make benchmark-tu39-space-changefeed
```
