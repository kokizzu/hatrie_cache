# T-U39 Named-Space Changefeed

`hatDataStructure.SpaceChangefeed` is an opt-in, bounded change publication
for callers that own a named space. It fills the gap between local key
watchers and SQL-specific publications: changes carry a space identity,
schema version, contiguous sequence, source frontier, and detached binary
key/before/after images.

## Usage

```go
feed, err := hatDataStructure.NewSpaceChangefeed(
    "orders",
    7,
    hatDataStructure.SpaceChangefeedOptions{
        MaxHistoryBatches: 256,
        MaxPendingBatches: 64,
    },
)
if err != nil {
    return err
}

if err := feed.Append(hatDataStructure.SpaceChangefeedBatch{
    Sequence:      1,
    Frontier:      42,
    SchemaVersion: 7,
    Changes: []hatDataStructure.SpaceChange{{
        Operation: hatDataStructure.SpaceChangeInsert,
        Key:       []byte("order-1"),
        After:     []byte(`{"status":"new"}`),
    }},
}); err != nil {
    return err
}

subscription, err := feed.Subscribe(ctx, hatDataStructure.SpaceChangefeedCheckpoint{})
if err != nil {
    return err
}
defer subscription.Close()

for batch := range subscription.Updates() {
    // Apply the downstream side effect, then persist this checkpoint.
    if err := subscription.Ack(hatDataStructure.SpaceChangefeedCheckpoint{
        Sequence:      batch.Sequence,
        Frontier:      batch.Frontier,
        SchemaVersion: batch.SchemaVersion,
    }); err != nil {
        return err
    }
}
```

`Append` accepts only the next sequence, the fixed schema version, and a
nondecreasing frontier. It validates the batch before publishing it. The feed
retains a bounded history and a new subscriber replays from its checkpoint;
checkpoint expiry is explicit when the requested sequence has fallen outside
retention. Checkpoints use a compact CRC32C-protected `SCF1` binary encoding
through `MarshalBinary` and `UnmarshalBinary`.

Each accepted batch is detached from caller-owned byte slices. Each subscriber
also receives an independent packed copy, so mutating an input or delivered
batch cannot corrupt retained history or another consumer. A slow subscriber
never blocks the publisher: when its bounded queue is full it is closed with
`ErrSpaceChangefeedBackpressure`, while the batch remains in retained history
for a later checkpointed resubscription.

`Complete` closes the feed normally after delivering its terminal batch.
`Close` rejects new batches and terminates active subscribers with
`ErrSpaceChangefeedClosed`. The feed does not persist rows or checkpoints;
callers own durable storage, downstream transactions, and replay policy.

## Defaults and scope

The primitive is caller-created, so ordinary data structures and SQL paths pay
no cost. Defaults are bounded at 256 history batches, 4,096 changes per batch,
64 pending batches per subscriber, 1,024 subscribers, and 4 MiB of key/image
payload per batch. Operators can lower these limits for memory-sensitive
spaces. It is intentionally schema-version pinned; an incompatible schema
should create a new feed or be coordinated by the caller.

## Verification

Focused tests cover detached byte ownership, replay and acknowledgement,
checkpoint checksum validation, sequence/schema/size bounds, expiry,
backpressure, completion, option validation, and concurrent snapshots. The
package was also run under the race detector and `go vet`.

The benchmark report is in `TR039_BENCHMARK_RAW.txt` and the cumulative table
is in `BENCHMARK.md`.
