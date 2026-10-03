# Durable SQL Publication Subscriptions

`hatSql.SQLPublication` supports an opt-in restartable replay consumer through
`SubscribeDurable`. The caller supplies a durable implementation of
`SQLPublicationSubscriptionStore`; the library never writes credentials or
chooses a storage backend.

```go
subscription, err := publication.SubscribeDurable(ctx, hatSql.SQLPublicationDurableSubscriptionOptions{
	SubscriptionID: "orders-sink",
	Store:          store,
})
if err != nil {
	return err
}
defer subscription.Close() // resumable disconnect

for batch := range subscription.Updates() {
	if err := applyBatch(batch); err != nil {
		return err
	}
	if err := subscription.AckContext(ctx, hatSql.SQLPublicationCheckpoint{
		Revision: batch.Revision,
		Frontier: batch.Frontier,
	}); err != nil {
		return err
	}
}
```

The store record contains the publication name, subscription ID, last
acknowledged checkpoint, and one lifecycle state:

- `active`: `Close` may later resume after the checkpoint.
- `cancelled`: `Cancel` has durably terminated the ID; a later resume is
  rejected.
- `completed`: the terminal publication batch was acknowledged.

`SubscribeDurable` loads an existing active record before replay. A new record
is committed before the subscription handle is returned. `AckContext` commits
the new checkpoint before updating in-memory state, so a failed store write is
retryable and cannot silently skip records. `Cancel` commits the cancelled
state before closing the subscription; a failed cancellation leaves the
consumer active.

The store's `Commit` operation must atomically replace one record. Use a
transaction, compare-and-swap, or equivalent single-key atomic write. Keep
publication history available at least through the oldest checkpoint that may
resume; an expired checkpoint returns `ErrSQLPublicationCheckpointExpired` and
requires a fresh snapshot. Durable records should be scoped by publication
name and subscription ID, and IDs are validated as bounded UTF-8 text.
