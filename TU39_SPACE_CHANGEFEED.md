# TU39 Space Changefeed

`CommandJournal.SubscribeSpaceChangefeed` exposes a named, versioned changefeed over the existing durable command journal.

## Example

```go
feed, err := journal.SubscribeSpaceChangefeed(ctx, hatCache.SpaceChangefeedOptions{
	Name:          "orders",
	SchemaVersion: 1,
	KeyPrefix:     "orders/",
	Buffer:        256,
})
if err != nil {
	return err
}
defer feed.Close()

for {
	event, ok, err := feed.Next(ctx)
	if err != nil {
		return err
	}
	if !ok {
		break
	}
	consume(event.Request.Key, event.Operation, event.Sequence)
}
```

`KeyPrefix` is the physical boundary of the logical space. An empty prefix intentionally follows every journal key. The feed keeps the journal's global sequence, so unrelated keys may create sequence gaps without weakening ordering among matching events.

## Checkpointed Reconnect

`Checkpoint` advances only after `Next` returns an event. Persist it with the consumer's own checkpoint store and resume with the same name, schema version, and key prefix:

```go
checkpoint := feed.Checkpoint()

resumed, err := journal.ResumeSpaceChangefeed(ctx, checkpoint, hatCache.SpaceChangefeedOptions{
	Name:          checkpoint.Name,
	SchemaVersion: checkpoint.SchemaVersion,
	KeyPrefix:     "orders/",
	Buffer:        256,
})
```

The name and schema version are validated on reconnect. A mismatched schema fails with `ErrSpaceChangefeedSchemaMismatch`; a mismatched name or conflicting sequence fails with `ErrSpaceChangefeedCheckpointMismatch`.

## Event Semantics

- Events are delivered in journal sequence order for the selected key prefix.
- `SpaceChangefeedUpsert` is the default operation.
- `SpaceChangefeedRetraction` and `Retraction=true` identify built-in delete commands such as `DEL`, `DELCF`, `DELRT`, `INTERNALDEL`, `RBDEL`, `REMRB`, and `REMSET`.
- Applications can add custom retraction commands with `RetractionCommands`.
- `Request` is the original journal request; the feed does not deep-copy its nested values.
- `Buffer` remains bounded. A slow consumer receives the existing `ErrCommandJournalSubscriptionOverflow` instead of causing an unbounded queue.
- Replay and compaction errors from the underlying journal subscription are returned by `Next` and `Err`.

The existing low-level `Subscribe` and `SubscribeSpace` APIs are unchanged.

## Measurement

The benchmark isolates one in-process channel receive and excludes disk I/O. Five one-second samples on an AMD Ryzen 9 5950X:

| Path | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Raw `CommandJournalRecord` receive | 45.71 | 0 | 0 | 1.00x |
| Named space `Next` | 89.84 | 0 | 0 | 1.97x |

The named envelope adds about 44 ns/event with no heap allocation. The background-context fast path reduced the named feed from a pre-optimization median of 107.7 ns/op to 89.84 ns/op, or about 1.20x faster. Run `make benchmark-tu39` for the paired samples and `make benchmark-tu39-baseline` for the raw baseline.

Focused correctness coverage is in `hat/hatCache/tu39_space_changefeed_test.go`, including replay, filtering, checkpoint reconnect, schema fencing, and bounded overflow.
