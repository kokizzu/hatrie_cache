# T230 On-Replace Changefeed Hooks

T230 adds an opt-in post-mutation hook to `hatDataStructure.Space`. It fills
the integration boundary between the storage space and the existing bounded
`hatReplication.SpaceChangefeed` publisher.

## API

Set `SpaceOptions.OnReplace` when constructing a space:

```go
feed, _ := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{
	Space: "orders", SchemaVersion: "v1",
})
space, _ := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
	OnReplace: func(change hatDataStructure.SpaceReplace) {
		op := hatReplication.SpaceChangefeedInsert
		if change.Delete {
			op = hatReplication.SpaceChangefeedDelete
		} else if change.Exists {
			op = hatReplication.SpaceChangefeedUpdate
		}
		_, _ = feed.Publish(hatReplication.SpaceChangefeedEvent{
			Operation: op,
			Key:       []byte(change.Key),
			Before:    change.OldValue,
			After:     change.NewValue,
		})
	},
})
```

The `SpaceReplace` image contains the key, old value, new value, `Exists`, and
`Delete` flags. The hook fires for successful inserts, replacements, and
deletes. Missing-key deletes and writes rejected by normal validation or a
`BeforeReplace` callback do not emit events.

## Ordering and ownership

- `OnReplace` runs after the selected engine accepts the mutation.
- The hook runs under the space's mutation serialization, so callback order
  follows successful mutation order.
- Old and new byte slices are independent copies. Mutating the callback event
  cannot mutate stored data or the caller's input.
- The callback cannot reject an already committed mutation. Use
  `BeforeReplace` for validation or conflict policy.
- The callback must not call back into the same `Space`; it runs while the
  mutation lock is held. It should publish quickly and hand off slow work to a
  separate bounded consumer.
- A feed publication error is observed by the callback but cannot be returned
  from `Space.Put` or `Space.Delete`, because the storage mutation has already
  succeeded. Applications needing durable delivery must define retry or
  backpressure policy around the feed.

`OnReplace` is nil by default. No event-image allocation or callback dispatch
is added to the default path.

## Cost

The focused memtx benchmark repeats a five-byte replacement. Values are
medians of five `-benchmem -cpu=1` samples on Linux/amd64 with an AMD Ryzen 9
5950X:

| Workload | CPU | Memory | Relative CPU |
| --- | ---: | ---: | ---: |
| Existing path before T230 | 48.40 ns/op | 8 B/op, 1 alloc/op | 1.00x |
| Hook disabled after T230 | 50.38 ns/op | 8 B/op, 1 alloc/op | 1.04x |
| Hook enabled, no-op callback | 96.66 ns/op | 24 B/op, 3 allocs/op | 1.92x |

The enabled path pays for copied event images and the serialized callback. The
default remains disabled and retains the original allocation profile. Raw
samples are recorded in [BENCHMARK.md](BENCHMARK.md#t230-on-replace-changefeed-hooks).
