# M-U34 Durable Subscription Checkpoints

`SQLPublication` already retains a bounded history and validates contiguous
replay. The opt-in durable subscription helpers add the missing restart and
cancellation handoff without changing ordinary subscriptions.

## Usage

Implement `SQLPublicationCheckpointStore` with durable, authenticated storage:

```go
type SQLPublicationCheckpointStore interface {
    Load(ctx context.Context, publication, consumer string) (hatSql.SQLPublicationCheckpoint, bool, error)
    Save(ctx context.Context, publication, consumer string, checkpoint hatSql.SQLPublicationCheckpoint) error
}
```

Use a stable consumer key when subscribing:

```go
subscription, err := publication.SubscribeDurable(ctx, "orders-worker", store)
if err != nil {
    return err
}
defer subscription.Close()

for batch := range subscription.Updates() {
    if err := applyDownstream(batch); err != nil {
        return err
    }
    if err := subscription.AckWithStore(ctx, store, hatSql.SQLPublicationCheckpoint{
        Revision: batch.Revision,
        Frontier: batch.Frontier,
    }); err != nil {
        return err
    }
}
```

For an intentional shutdown, persist the last acknowledged cursor before
closing the live subscription:

```go
if err := subscription.Cancel(ctx, store); err != nil {
    return err
}
```

`Cancel` leaves the subscription open when the store rejects the checkpoint,
so the caller can retry. `AckWithStore` accepts a repeated equal checkpoint;
this makes a failed store write retryable without skipping retained history.

## Guarantees And Limits

- `SubscribeDurable` loads the consumer cursor before replay and starts after
  the last acknowledged revision.
- Consumer keys are scoped by publication name, so independent consumers do
  not share a cursor.
- The existing bounded-history expiry checks still apply. A cursor older than
  retained history returns `ErrSQLPublicationCheckpointExpired` and requires a
  fresh snapshot or resynchronization.
- The store owns encryption, authorization, durability, and monotonic Save
  behavior. Checkpoint persistence is not an atomic transaction with the
  downstream side effect; acknowledge only after that side effect is durable.
- `Subscribe`, `Ack`, and `Close` retain their existing behavior and do not
  call a checkpoint store.

## Measurement

Five benchmark samples of one replay/ack/close cycle on the same run:

| Path | ns/op samples | B/op | allocs/op |
| --- | --- | ---: | ---: |
| Existing in-memory subscription | 3043, 2943, 2866, 2934, 2760 | 7,224 | 21 |
| Durable subscription with in-memory store | 3058, 2943, 2968, 2981, 3099 | 7,240 | 22 |

The opt-in protocol adds one allocation and 16 bytes in this small in-memory
store benchmark. The durable store's actual I/O cost is caller-dependent; the
ordinary subscription path has no new work.
