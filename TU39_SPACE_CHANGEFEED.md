# T-U39 Named Space Changefeed

`hatReplication.SpaceChangefeed` is an opt-in, in-process changefeed for one
logical space. It gives consumers a stable schema identity, monotonically
increasing sequences, bounded replay history, consumer-owned checkpoints, and
explicit subscriber overflow instead of silently dropping events.

It is an importable primitive. It does not open a listener, replicate over the
network, persist history, authenticate consumers, or change existing journal or
SQL subscription behavior.

## Example

```go
feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{
	Space:         "orders",
	SchemaVersion: "orders-v1",
})
if err != nil {
	return err
}
defer feed.Close()

subscription, err := feed.Subscribe(ctx, hatReplication.SpaceChangefeedSubscribeOptions{
	Checkpoint: checkpoint,
	Buffer:     256,
})
if err != nil {
	return err
}
defer subscription.Close()

for event := range subscription.Events() {
	if err := applyDurably(event); err != nil {
		return err
	}
	checkpoint, err = subscription.Advance(event.Sequence)
	if err != nil {
		return err
	}
}
if err := subscription.Err(); err != nil {
	return err
}
```

`Publish` fills in the feed space, schema version, and sequence when the event
omits them. `Key`, `Before`, and `After` are copied on publication and again
for each subscriber, so callers may reuse or mutate their input buffers after
the call.

## Limits And Defaults

| Setting | Default | Maximum | Meaning |
|---|---:|---:|---|
| `MaxEvents` | 4,096 | 1,048,576 | Retained event count. |
| `MaxSubscribers` | 256 | 65,536 | Active subscriber count. |
| `MaxBytes` | 64 MiB | 1 GiB | Retained key/before/after payload bytes. Go slice and channel overhead is not included. |
| `MaxEventBytes` | 1 MiB | 16 MiB | Maximum combined key/before/after payload for one event. |
| `Buffer` | 256 | 65,536 | Per-subscriber channel capacity. |

History is an in-memory ring. Once a limit is reached, the oldest event is
evicted. A checkpoint older than the retained history returns
`ErrSpaceChangefeedHistoryGap`; it never resumes from an unannounced position.

## Delivery Contract

- Sequences start at 1 and increase only after successful validation.
- Events for one feed are delivered in sequence order.
- `Advance` records the greatest sequence durably applied by the consumer. It
  must be called after the consumer commits its own side effect.
- `Subscribe` rejects a checkpoint from another space, a checkpoint ahead of
  the feed, or a schema version mismatch.
- A replay larger than the requested buffer returns
  `ErrSpaceChangefeedReplayLimit`; increase the buffer or use a later
  checkpoint.
- `Publish` never blocks on a subscriber. If its channel is full, that
  subscriber is closed with `ErrSpaceChangefeedOverflow`, and the event stays
  in the bounded replay history.
- A canceled subscription context closes the event channel and exposes the
  context error through `Err`.

This makes slow consumers observable and recoverable, but it is not a durable
replication protocol. Persist checkpoints and pair them with an external
snapshot/WAL or journal recovery plan when loss across process restart matters.

## Resource And Security Notes

The feed stores caller-provided payload copies up to the configured payload
limits, plus Go slice, ring, and channel overhead. Keep limits aligned with the
process memory budget and do not put secrets in events unless the surrounding
transport/storage path provides encryption and access control. The package
does not provide authorization, encryption, or network framing.

## Benchmark

The focused benchmark runs for one second per sample on Linux/amd64 with an
AMD Ryzen 9 5950X. The direct baseline is a preallocated fixed-ring write of
the same small event shape; it measures the lower bound for event retention,
not a durable network or journal implementation.

| Path | Median ns/op | B/op | allocs/op | Relative CPU to direct append |
|---|---:|---:|---:|---:|
| Direct retained append baseline | 1.019 | 0 | 0 | 1.0x |
| Changefeed publish, no subscriber | 110.6 | 32 | 2 | 109x |
| Changefeed publish, one subscriber | 230.3 | 64 | 4 | 225x |

The cost is intentional and bounded: the feature provides sequence assignment,
payload isolation, replay history, and explicit backpressure. It is not enabled
on existing write paths by default. Raw output is recorded in `BENCHMARK.md`.
