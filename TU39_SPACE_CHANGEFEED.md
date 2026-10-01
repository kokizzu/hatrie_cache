# T-U39 Space Changefeed

`hatCache.CommandJournalSpaceFeed` is an opt-in named-space changefeed built
on the durable command journal.

## Contract

- `SubscribeSpaceFeed` returns ordered events for one exact logical space.
- Each event and checkpoint carries `SchemaVersion` (`1`) and `Space`.
- `Next` delivers one event at a time. Call `Ack(sequence)` only after the
  event is durably applied by the consumer.
- `Checkpoint` returns the last acknowledged sequence. Persist it and pass it
  back in `CommandJournalSpaceFeedOptions.Checkpoint` to resume exclusively
  after that sequence.
- The default `CommandJournalSpaceFeedFailFast` policy preserves every
  matching record and terminates with the existing overflow error when the
  bounded buffer is full.
- `CommandJournalSpaceFeedCoalesceLatest` is explicitly lossy: it keeps the
  newest pending record for the space and should only be used when the latest
  state is sufficient.

```go
feed, err := journal.SubscribeSpaceFeed(ctx, "orders", hatCache.CommandJournalSpaceFeedOptions{})
if err != nil {
    return err
}
defer feed.Close()

for {
    event, err := feed.Next(ctx)
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        return err
    }
    if err := apply(event.Record); err != nil {
        return err
    }
    if err := feed.Ack(event.Sequence); err != nil {
        return err
    }
    checkpoint := feed.Checkpoint()
    if err := saveCheckpoint(checkpoint); err != nil {
        return err
    }
}
```

The checkpoint is a consumer cursor, not a journal mutation. Persist it only
after the corresponding record is durably applied. A checkpoint from another
space or schema version is rejected. A checkpoint ahead of the local journal
is also rejected rather than silently starting at the current tail.

## Cost

The feature adds one small wrapper allocation per feed and one event envelope
per `Next` call. Ordinary journal writes and ordinary subscriptions do not
pay for it. Run the paired benchmark with:

```text
make benchmark-round41-space-feed
```

The benchmark compares raw space subscription delivery with the explicit
versioned `Next`/`Ack` path and reports CPU, allocations, and bytes per
operation.
