# M-U34 Historical Subscription Cancellation

`CommandJournal.SubscribeHistoricalCommandJournal` adds a restart-safe,
bounded replay session for consumers that need to stop in the middle of a
historical journal replay.

## Example

```go
subscription, err := journal.SubscribeHistoricalCommandJournal(ctx,
	 hatCache.CommandJournalHistoricalSubscriptionOptions{
		SourceID:    "orders-projector",
		Store:       checkpointStore,
		KeyPrefix:   "orders/",
		ReplayLimit: 4096,
		Buffer:      256,
	})
if err != nil {
	return err
}
defer subscription.Close()

for {
	record, ok, err := subscription.Next(ctx)
	if err != nil {
		return err
	}
	if !ok {
		break
	}
	if err := apply(record); err != nil {
		return err
	}
	if _, err := subscription.Commit(ctx); err != nil {
		return err
	}
}
```

`Store` uses the existing `CommandJournalSourceCheckpointStore` contract. The
source ID identifies the consumer checkpoint. Reopening the same subscription
with the same source ID and store starts after the last successful commit.

## Cancellation And Recovery

- `Next` marks a record as delivered but does not persist progress.
- `Commit` persists exactly the delivered journal sequence, never the current
  journal tail. This prevents a fast producer from causing unconsumed history
  to be skipped.
- If cancellation, process failure, or checkpoint-store failure occurs before
  `Commit`, the record is replayed on restart. This is deliberate at-least-once
  behavior; the consumer applies the record before acknowledging it.
- `CommitJournalSequence` uses the journal persistence barrier and rejects a
  sequence ahead of the local durable journal.
- `Buffer` and replay limits are inherited from the existing bounded journal
  subscription. No unbounded queue or extra delivery goroutine is introduced.
- Existing `Commit(ctx, sourceID, offset)` behavior is unchanged for source
  ingestion callers that intentionally checkpoint the current applied tail.

## Measurement

Five one-second samples on an AMD Ryzen 9 5950X using an in-process bounded
channel fixture. This isolates delivery overhead and excludes checkpoint-store
I/O:

| Path | Samples (ns/op) | Median | B/op | allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Raw journal record receive | 44.43, 46.53, 47.21, 46.01, 44.76 | 46.01 | 0 | 0 | 1.00x |
| Historical subscription `Next` | 67.08, 71.01, 66.75, 64.41, 63.83 | 66.75 | 0 | 0 | 1.45x |
| Legacy tail checkpoint commit | 16.58, 16.14, 17.47, 18.41, 18.14 | 17.47 | 0 | 0 | 1.00x |
| Exact sequence checkpoint commit | 15.20, 16.36, 15.07, 15.09, 16.84 | 15.20 | 0 | 0 | 0.87x |

The session adds about 21 ns/event and no heap allocation. Exact-sequence
checkpointing is within the noise of the legacy tail checkpoint path and was
15% faster in this sample set. Durable `Commit` cost depends on the caller's
checkpoint store and is paid only at the explicit acknowledgement boundary.
Run `make benchmark-mu34` for the current paired result and
`make benchmark-mu34-baseline` for the raw baseline.
