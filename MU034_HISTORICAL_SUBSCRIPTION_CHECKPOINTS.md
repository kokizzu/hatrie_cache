# M-U34 Historical Subscription Checkpoints

`CommandJournal.Subscribe` remains unchanged for callers that do not need
restart recovery. Consumers that must resume an interrupted historical replay
can opt into:

```go
store, err := hatCache.NewFileCommandJournalCheckpointStore("state/orders.checkpoint")
if err != nil {
	return err
}
subscription, err := journal.SubscribeWithCheckpoint(ctx, store, hatCache.CommandJournalSubscribeOptions{
	ReplayLimit: 10000,
})
if err != nil {
	return err
}
defer subscription.Close()

for record := range subscription.Records() {
	if err := process(record); err != nil {
		return err
	}
	if err := subscription.Acknowledge(ctx, record.Sequence); err != nil {
		return err
	}
}
```

`SubscribeSpaceWithCheckpoint` provides the same contract for one logical
space. The checkpoint is loaded before replay, and a checkpoint ahead of the
current journal is rejected instead of silently skipping data.

Acknowledgement is at-least-once: a record is eligible for replay until the
consumer explicitly acknowledges it. Acknowledge only after the application
has completed processing. Acknowledgements are monotonic and idempotent, and
failed saves do not advance the in-memory checkpoint. A consumer can
acknowledge the highest sequence after processing a batch to reduce durable
write frequency.

`FileCommandJournalCheckpointStore` writes a checksummed, private `0600` file
through a temporary file, fsync, atomic rename, and directory fsync. A missing
file means sequence zero. Applications that already have a transactional or
remote checkpoint store can implement `CommandJournalCheckpointStore` instead.

The feature is opt-in. Existing subscriptions do not acquire checkpoint locks
or perform checkpoint I/O.
