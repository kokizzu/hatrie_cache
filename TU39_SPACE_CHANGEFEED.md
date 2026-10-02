# Space Changefeed

`hat/hatReplication.SpaceChangefeed` is an opt-in, bounded changefeed for one
named space. It packages the parts needed by a SQL subscription, a CDC adapter,
or a replication worker without forcing a transport or storage choice:

- every event has an ordered sequence, space name, schema version, operation,
  key, value, and timestamp;
- history is a fixed-size ring, so retention is bounded;
- pull readers and push-style subscribers resume from a checkpoint;
- replay never silently drops events: an expired checkpoint or too-small replay
  buffer returns an error;
- a full subscriber queue applies lossless backpressure to `Publish` until the
  subscriber receives an event or the publisher context is canceled;
- event key and value bytes are copied on publish and receive, so callers can
  reuse their input buffers safely.

The feed is not created by the server automatically. Existing applications have
no new work or memory cost unless they construct one, which keeps the default
off. The caller remains responsible for durable checkpoint storage,
authorization, transport, and converting a database mutation into an event.

## Example

```go
feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{
    Space:         "orders",
    SchemaVersion: 7,
    Capacity:      4096,
})
if err != nil {
    return err
}

checkpoint, err := hatReplication.NewSpaceChangefeedCheckpoint("orders", 7, 0)
if err != nil {
    return err
}
subscription, err := feed.Subscribe(checkpoint, 128)
if err != nil {
    return err
}

_, err = feed.Publish(ctx, hatReplication.SpaceChangefeedEvent{
    Space:         "orders",
    SchemaVersion: 7,
    Operation:     "upsert",
    Key:           []byte("order:42"),
    Value:         []byte(`{"status":"paid"}`),
})
if err != nil {
    return err
}

event, err := subscription.Receive(ctx)
if err != nil {
    return err
}
checkpoint = subscription.Checkpoint()
encoded, err := checkpoint.MarshalBinary()
if err != nil {
    return err
}
_ = event
_ = encoded
```

`Space` and `SchemaVersion` on a published event must identify the feed. The
feed assigns the sequence and timestamp. Use an operation such as `insert`,
`upsert`, `update`, or `delete`; the library preserves the operation string but
does not impose SQL semantics on it.

## Checkpoints and Recovery

`SpaceChangefeedCheckpoint.MarshalBinary` produces a bounded deterministic
`scf1` frame containing the space, schema version, sequence, and a CRC32C. Store
that frame durably only after the consumer has applied the corresponding event.
On restart, decode it with `UnmarshalSpaceChangefeedCheckpoint` and subscribe
from the returned position. If the ring has already evicted the position,
rebuild from a snapshot or another durable source instead of accepting a gap.

The checksum detects corruption; it is not an authentication mechanism. Protect
checkpoint and event transport with the application's access control and secure
channel.

## Backpressure and Retention

The feed retains at most `Capacity` events, defaulting to 1024 and capped at
65536. A subscriber queue defaults to 128 events and is capped at 4096. A slow
subscriber therefore slows publishers rather than causing an invisible data
loss. A publisher with a deadline can use `context.WithTimeout` and decide
whether to retry, disconnect the subscriber, or fail the write path.

Stateless consumers can use `Read(checkpoint, limit)` without registering a
subscriber. It returns the retained events and the next checkpoint. This is
useful for a polling worker, while `Subscribe` is useful when a consumer needs
explicit queue backpressure.

## Benchmark

The following is one representative run on an AMD Ryzen 9 5950X 16-Core
Processor with `make benchmark-chg22-changefeed`. The legacy fixture mirrors
the existing unbounded `hatSql.ChangeLog` append shape. The feed benchmarks use
the same package and event payload:

| Path | ns/op | B/op | allocs/op | Relative time | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Legacy unbounded append | 453.7 | 589 | 0 | 1.00x | 1.00x |
| Bounded feed publish, no subscriber | 178.0 | 144 | 5 | 0.39x (2.55x faster) | 0.24x (4.09x lower) |
| Bounded feed publish plus receive | 508.1 | 400 | 11 | 1.12x (0.89x) | 0.68x (1.47x lower) |

The no-subscriber row is useful for measuring bounded append overhead, but it is
not a replacement for delivery. The publish-plus-receive row is the relevant
comparison when a consumer is attached: it costs about 12% more CPU and adds
allocations for synchronization, queueing, and copy isolation, while retaining
bounded memory and explicit lossless delivery semantics. Existing legacy
`ChangeLog` users are unchanged because this feature is opt-in.

Benchmark numbers are machine- and workload-dependent. Re-run the Makefile
target before using them for capacity planning.

## Verification

The feature's focused workflow is:

```text
make red-chg22-changefeed
make baseline-chg22-changefeed
make test-chg22-changefeed
make race-chg22-changefeed
make vet-chg22-changefeed
make benchmark-chg22-changefeed
```

The focused tests cover ordering, bounded replay, checkpoint encoding and
corruption detection, checkpoint expiry, subscriber backpressure, context
cancellation, close behavior, input/output byte ownership, and schema/space
validation.
