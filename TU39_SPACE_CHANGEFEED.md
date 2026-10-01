# T-U39 Space Changefeed

`hatReplication.SpaceChangefeed` is an opt-in, bounded replay log for one named space. It fills the gap between a local key watcher and a durable, reconnectable stream without forcing a SQL serializer or network protocol into the replication package.

## Contract

- `SpaceChangefeedOptions` requires a space name and schema version. Capacity defaults to `1024` events and is capped at `65536`.
- `Publish` appends an atomically visible batch. A batch larger than the configured capacity is rejected before mutation.
- Payloads are opaque copied byte slices for key, before-image, and after-image. One change is capped at `16 MiB`.
- `Subscribe` binds a consumer to the exact space and schema version and starts after an acknowledged checkpoint.
- `Next` preserves publication order. `Ack` advances the durable consumer position and allows old events to be evicted once every active subscriber has passed them.
- A full log waits for consumer acknowledgement and honors the publisher context. No per-subscriber or per-publisher goroutine is created.
- Reconnecting from a retained checkpoint replays the missing events. A checkpoint older than the retained window returns `ErrSpaceChangefeedHistoryGone` instead of silently skipping data.
- `Close` rejects new publications, wakes waiters, and drains already retained events for existing subscriptions.

The feed is memory-only. Persist `SpaceChangefeedSubscription.Checkpoint()` in the caller's checkpoint store and recreate the subscription after reconnect. The caller also owns row encoding, authentication, transport, and integration with SQL or command mutation paths.

## Example

```go
feed, _ := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{
    Space: "orders", SchemaVersion: 1, Capacity: 1024,
})
consumer, _ := feed.Subscribe(feed.InitialCheckpoint())
_, _ = feed.Publish(ctx, []hatReplication.SpaceChangefeedChange{{
    Key: []byte("order-1"),
    After: []byte(`{"status":"paid"}`),
    Operation: hatReplication.SpaceChangefeedUpdate,
}})
event, _ := consumer.Next(ctx)
// Persist consumer.Checkpoint() only after the downstream write succeeds.
_ = consumer.Ack(event.Checkpoint.Sequence)
```

## Measured Cost

On the repository's AMD Ryzen 9 5950X Linux host, Go benchmark runs used `-benchmem -count=5`:

| Path | Median time | Memory | Allocations |
| --- | ---: | ---: | ---: |
| direct assignment control | 1.20 ns/op | 0 B/op | 0 allocs/op |
| publish + next + ack, one event | 223.1 ns/op | 128 B/op | 3 allocs/op |

The one-event path is intentionally compared with a no-op control only to show semantic overhead; it is not a claim that replay is faster than assignment. Batching multiple changes amortizes the feed lock and wake-up work. Retention measurement at 1024 events used about `200,472 B` (`195.8 B/event`) for small payloads, including the bounded event slice and copied payloads.

The implementation was reduced from `373.3 ns/op, 384 B/op, 9 allocs/op` during the initial test-first version to the final `223.1 ns/op, 128 B/op, 3 allocs/op` through packed payload storage and a condition-variable fast path for non-cancelable waits. Context-aware waits retain the notification-channel path for cancellation correctness.
