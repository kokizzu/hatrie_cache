# Historical Subscription Cancellation

`hatSql.SQLPublication` subscriptions now treat cancellation as a resumable
replay boundary.

## Behavior

- `Subscribe` observes the supplied context for the lifetime of the active
  subscription, not only during construction.
- Context cancellation closes the update and done channels and exposes the
  original `context.Canceled` or `context.DeadlineExceeded` through `Err`.
- `SQLPublicationSubscription.Cancel` provides an explicit cancellation path
  with `ErrSQLPublicationCanceled`.
- `Ack` remains the durable boundary. Cancellation never advances it, so a
  consumer can persist `Checkpoint()` and resume from that exact acknowledged
  position.
- `SQLPublicationCheckpoint.MarshalBinary` and `UnmarshalBinary` provide the
  fixed 24-byte CRC-protected record for a small checkpoint store.

## Example

```go
ctx, stop := context.WithCancel(context.Background())
subscription, err := publication.Subscribe(ctx, savedCheckpoint)
if err != nil {
    return err
}

for {
    select {
    case batch, ok := <-subscription.Updates():
        if !ok {
            return subscription.Err()
        }
        if err := applyDurably(batch); err != nil {
            return err
        }
        if err := subscription.Ack(SQLPublicationCheckpoint{
            Revision: batch.Revision,
            Frontier: batch.Frontier,
        }); err != nil {
            return err
        }
        if err := saveCheckpoint(subscription.Checkpoint()); err != nil {
            return err
        }
    case <-shutdown:
        stop()
        <-subscription.Done()
        return subscription.Err()
    }
}
```

Persist the checkpoint only after the downstream side effect is durable. A
restart from the last persisted checkpoint is therefore at-least-once and may
replay the batch after that checkpoint, but cannot skip an acknowledged batch.

The normal `context.Background()` path has no watcher allocation. A
cancellable context adds one bounded watcher allocation and goroutine for the
active subscription; the watcher exits when either the context or subscription
ends.
