# M-U34 Historical Subscription Checkpoints

`hatSql.HistoricalQuerySubscription` is an opt-in acknowledgement boundary for
historical query replay. It addresses the restart gap between a query result
being delivered and the downstream side effect being durable.

## Contract

Create one with `QuerySubscriptions.SubscribeHistorical`. The initial query
result becomes the baseline checkpoint and is not emitted a second time. For
each later `QuerySubscriptionSnapshot`:

1. Apply the snapshot to the downstream consumer.
2. Call `Ack(snapshot)` only after that side effect is durable.
3. Persist `Checkpoint()` or its `MarshalBinary()` bytes.

`ResumeHistorical` restores the last acknowledged full result without reading
the source during restart. The next `NotifyChangedAt` refresh starts from that
frontier, so an interrupted replay cannot silently acknowledge a future
boundary. A stale or fabricated snapshot is rejected unless its ID, revision,
frontier, completion state, and current result all match the live boundary.

`Cancel()` marks the last acknowledged checkpoint as terminal before closing
the live subscription. `Close()` only removes the in-memory subscription and
does not persist cancellation. Completed and canceled checkpoints are not
resumable; this prevents an operator restart from accidentally replaying a
terminal stream.

The checkpoint contains the query result data needed for reconciliation:
query ID, columns, rows, `HasMore`, and `NextCursor`. Explain plans and runtime
statistics are intentionally excluded. The checkpoint is a caller-owned
durable artifact; no file or database store is opened by this API.

## Binary Format

`MarshalBinary` uses deterministic `HQS1` bytes with typed values from the
existing HDF1 row codec and a CRC32C checksum. Default bounds are:

| Bound | Default |
| --- | ---: |
| Encoded checkpoint | 64 MiB |
| Rows | 1,048,576 |
| Fields per row / result columns | 1,024 |
| Nested value depth | 16 |

The checksum detects corruption but does not authenticate an attacker. Store
checkpoint bytes in an authenticated, access-controlled location, and encrypt
them when result rows contain sensitive data.

## Example

```go
subscription, err := registry.SubscribeHistorical(ctx, definition, resolver, hatSql.QueryOptions{})
if err != nil {
    return err
}
defer subscription.Close()

for update := range subscription.Updates() {
    if err := applyDurably(update); err != nil {
        return err
    }
    if err := subscription.Ack(update); err != nil {
        return err
    }
    checkpointBytes, err := subscription.Checkpoint().MarshalBinary()
    if err != nil {
        return err
    }
    if err := checkpointStore.Save(ctx, checkpointBytes); err != nil {
        return err
    }
}

checkpointBytes, err := checkpointStore.Load(ctx)
if err != nil {
    return err
}
var checkpoint hatSql.QuerySubscriptionCheckpoint
if err := checkpoint.UnmarshalBinary(checkpointBytes); err != nil {
    return err
}
subscription, err = registry.ResumeHistorical(definition, checkpoint)
```

The caller must persist the checkpoint atomically with the downstream effect
it represents. This API supplies the boundary and validation; it does not
pretend to provide a distributed transaction with an external sink.

## Measurements

Command:

```sh
make benchmark-mu034
```

Five `-count=5` samples were collected on Linux/amd64 with an AMD Ryzen 9
5950X. The ordinary control and the opt-in checkpoint control both clone one
two-column, one-row result.

| Workload | Median | Memory | Result |
| --- | ---: | ---: | --- |
| Ordinary `Snapshot` control | 294.9 ns/op | 376 B/op, 4 allocs/op | Existing path |
| Historical `Checkpoint` | 307.6 ns/op | 376 B/op, 4 allocs/op | 1.04x the control; opt-in only |
| HQS1 binary encode | 236.4 ns/op | 160 B/op, 2 allocs/op; 42 wire bytes | 3.60x faster than JSON |
| JSON compatibility encode | 851.8 ns/op | 448 B/op, 7 allocs/op; 134 wire bytes | Baseline format |

The paired clean-base frontier control remained allocation-identical at
`160 B/op` and `1 alloc/op`; median timing was about `171 ns/op` before and
`172 ns/op` after, within measurement noise. The default subscription path
does not allocate checkpoint state or encode bytes.

Focused correctness, race, and vet commands are exposed by `make verify-mu034`.
