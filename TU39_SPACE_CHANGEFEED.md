# T-U39 Named-Space Changefeed

`hatReplication.SpaceChangefeed` is an opt-in ordered feed for one named
space. It provides the contract around already-committed transitions; it does
not modify storage writers or automatically observe a table.

## Contract

Create a feed with a non-zero `SchemaVersion`. Every event must use that exact
version, so a consumer never decodes a payload under the wrong tuple layout.
The default retention is 1,024 events and the default encoded key plus
before/after payload bound is 1 MiB. Both bounds are configurable within fixed
maximums.

The feed accepts three transitions:

- `SpaceChangeInsert`: key and `After`, with no `Before`.
- `SpaceChangeUpdate`: key, `Before`, and `After`.
- `SpaceChangeDelete`: key and `Before`, with no `After`.

`AppendBatch` validates every event and publishes the batch only after all
events pass. Failed validation leaves the feed unchanged. Keys and payloads
are copied on append and on read, so a producer or consumer cannot mutate
retained history through a reused buffer.

## Replay and backpressure

`ReadAfter(after, limit)` returns bounded pages and advances a sequence cursor.
When retention has evicted the requested prefix it returns
`ErrSpaceChangefeedHistoryGap`, allowing the caller to request a fresh snapshot
instead of silently skipping rows. `WaitAfter` blocks with a caller-provided
context and wakes on the next append or close. It does not allocate one
goroutine per idle consumer, and producers never block on a slow consumer; the
consumer applies backpressure by choosing a page size and cursor rate.

`Checkpoint()` returns the feed's named-space sequence as the existing
`ChangefeedCheckpoint`. Persist it with `MarshalBinary` after the consumer has
successfully applied a page, then use `ReadFromCheckpoint` after reconnect.
The source storage, checkpoint store, initial snapshot, and replay-after-restart
policy remain caller-owned; the in-memory feed intentionally is not a second
WAL.

## Usage

```go
feed, err := hatReplication.NewSpaceChangefeed(
    hatReplication.SpaceChangefeedOptions{
        Space:         "orders",
        SchemaVersion: 7,
        Capacity:      4096,
        MaxChangeBytes: 1 << 20,
    },
)
if err != nil {
    return err
}
defer feed.Close()

page, err := feed.AppendBatch([]hatReplication.SpaceChange{
    {
        SchemaVersion: 7,
        Operation:     hatReplication.SpaceChangeUpdate,
        Key:           orderKey,
        Before:        oldOrder,
        After:         newOrder,
    },
})
if err != nil {
    return err
}
_ = page
```

Writers should call `Append` or `AppendBatch` only after the underlying write
has committed. The feed is suitable for an HTTP, gRPC, or compact-protocol
adapter, but transport authentication and endpoint lifecycle stay outside the
data-structure package.

## Measurements

On the benchmark host (AMD Ryzen 9 5950X), three 200 ms runs measured:

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Zero-copy committed-write baseline | 6.18 | 0 | 0 |
| Single `Append` before fast path | 261.0 | 368 | 7 |
| Single `Append` final fast path | 150.8 | 144 | 5 |
| `AppendBatch` of 32 | 4,257 | 8,048 | 131 |
| Replay 16 events | 895.1 | 2,048 | 33 |
| Checkpoint read | 8.78 | 0 | 0 |

The single-event fast path is 1.73x faster, uses 2.56x less allocated heap,
and uses 1.4x fewer allocations than the initial batch-routed implementation.
The absolute append cost is opt-in and includes payload ownership copies;
there is no default-path overhead when no feed is created.
