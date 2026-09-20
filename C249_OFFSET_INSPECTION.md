# C249 Kafka-Style Offset Inspection

`hatReplication.SpaceChangefeed.Inspect` provides a bounded, read-only view of
retained changefeed history after an exclusive sequence offset. It is useful
for health checks, lag inspection, and one-shot polling when creating a
consumer and changing its checkpoint would be unnecessary.

```go
inspection, err := feed.Inspect(ctx, hatReplication.SpaceChangefeedInspectOptions{
	AfterSequence: checkpoint.Sequence,
	MaxEvents:     128,
	MaxBytes:      1 << 20,
})
if err != nil {
	return err
}
for _, event := range inspection.Events {
	apply(event)
}
// Persist inspection.NextSequence - 1 only after apply succeeds.
```

`AfterSequence` is exclusive. `FirstAvailableSequence` identifies the oldest
retained event, `NextSequence` is the next sequence that would be published,
and `More` reports that the event or byte bound omitted retained events.
Returned key, before, and after payloads are cloned. The method does not
create a subscriber, advance a checkpoint, or change feed statistics.

Zero limits select bounded defaults of 128 events and 1 MiB of payload bytes.
The maximums are 4,096 events and 16 MiB. A stale offset returns
`ErrSpaceChangefeedHistoryGap`; an offset ahead of the feed returns
`ErrSpaceChangefeedCheckpointAhead`; and an event that cannot fit the byte
bound returns `ErrSpaceChangefeedInspectLimit`.

## Benchmark

The benchmark publishes 1,024 small upsert events, then reads 24 events after
sequence 1,000. Five 200 ms samples were run on an AMD Ryzen 9 5950X, Linux
amd64, with `make benchmark-c249-offset-inspection`.

| Operation | Median time | Heap | Allocs | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing subscription replay | 14,305 ns/op | 5,160 B/op | 53 allocs/op | Baseline |
| Read-only offset inspection | 9,494 ns/op | 3,840 B/op | 49 allocs/op | 1.51x faster, 1.34x lower heap, 1.08x fewer allocations |

Raw samples:

```text
BenchmarkC249ExistingSubscriptionInspection-32 15406 15169 14305 14087 14084 ns/op 5160 B/op 53 allocs/op
BenchmarkC249ReadOnlyOffsetInspection-32        9573  9556  9494  9479  9264 ns/op 3840 B/op 49 allocs/op
```

The inspection path is additive and does not replace durable consumer
checkpoints. It is bounded to prevent an untrusted offset request from
allocating an unbounded response.
