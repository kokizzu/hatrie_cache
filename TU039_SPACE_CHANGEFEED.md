# T-U39 Named-Space Changefeed

This is the Tarantool-inspired space changefeed gap. It adds an importable,
opt-in `hatReplication.SpaceChangefeed` for callers that need to publish row
mutations from a named space to one or more consumers.

## Contract

- One feed is bound to one non-empty space name and one positive schema version.
- `Publish` and `PublishBatch` assign monotone sequences. Batches validate and
  reserve capacity before any event becomes visible.
- Events carry an operation, key, and opaque before/after row bytes. All input
  and output byte slices are copied, so callers cannot mutate retained history.
- `Subscribe` opens an at-least-once cursor. `Read` advances delivery;
  `Ack` advances the retention checkpoint. Unacknowledged events are replayed
  after reconnect from `SpaceChangefeedCheckpoint`.
- Checkpoints use bounded deterministic `scp1` binary encoding and bind the
  cursor to both the space and schema version.
- Retention is bounded by event count and payload bytes. Events are evicted
  only after every active subscriber has acknowledged them. If a subscriber
  prevents eviction, publication returns `ErrSpaceChangefeedBackpressure`
  instead of silently dropping data.
- A cursor older than retained history returns `ErrSpaceChangefeedGap`, which
  makes snapshot recovery an explicit caller decision.
- `Wait` is context-aware and uses lazy notification channels. No background
  goroutine or listener is started, and existing SQL or cache defaults are
  unchanged.

## Example

```go
feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{
    Space: "orders", SchemaVersion: 3,
    MaxEvents: 4096, MaxBytes: 16 << 20,
})
if err != nil { /* handle invalid configuration */ }

consumer, err := feed.Subscribe(hatReplication.SpaceChangefeedSubscribeOptions{})
if err != nil { /* handle closed feed */ }

sequence, err := feed.Publish(hatReplication.SpaceChangefeedInput{
    Operation: hatReplication.SpaceChangefeedUpdate,
    Key:       []byte("order-42"),
    Before:    []byte(`{"state":"paid"}`),
    After:     []byte(`{"state":"shipped"}`),
})
if err != nil { /* retry after backpressure or route to a snapshot */ }

events, err := consumer.Read(ctx, 128)
if err == nil {
    // Apply events durably before acknowledging the highest applied sequence.
    err = consumer.Ack(events[len(events)-1].Sequence)
}
_ = sequence
```

`PublishBatch` is the preferred path when a source transaction produces
multiple changes: it either publishes the complete batch or publishes none of
it. The feed does not persist events, serialize transport frames, or perform a
space snapshot; those responsibilities remain with the owning storage and
transport layers.

## Measured Tradeoffs

The focused benchmark ran five `-benchmem` samples on an AMD Ryzen 9 5950X.
The first implementation routed `Publish` through the batch API and allocated
notification channels even when no consumer was waiting. The final path has a
single-event fast path and lazy notifications.

| Path | Initial median | Final median | Final memory | Final allocations | Change |
| --- | ---: | ---: | ---: | ---: | --- |
| Publish, read, ack one event | 433.2 ns/op; 536 B/op; 13 allocs/op | 230.0 ns/op; 160 B/op; 7 allocs/op | 160 B/op | 7 | 1.88x faster; 70.1% fewer bytes; 46.2% fewer allocations |
| Checkpoint decode | not measured initially | 23.97 ns/op | 8 B/op | 1 | bounded schema/checkpoint validation |
| Existing frontier-only advance | reference | 2.44 ns/op | 0 B/op | 0 | not equivalent: no payload, retention, copy, or consumer state |

The richer feed intentionally costs more than the existing frontier primitive:
it copies payloads for isolation, returns owned event bytes, tracks subscribers,
and enforces bounded retention. The implementation is disabled unless a caller
constructs it, has no idle goroutine, and makes backpressure explicit rather
than trading correctness for an unbounded queue.
