# MZ-013 Source Connector Checkpoints

`CommandJournalSourceCheckpointCoordinator` is an opt-in API for source
connectors that ingest records into a `CommandJournal`. It persists a binary
source offset together with the journal sequence that was fully applied when
the offset was saved.

## Contract

Implement `CommandJournalSourceCheckpointStore` with a durable backend:

```go
type CommandJournalSourceCheckpointStore interface {
	Load(context.Context, string) (CommandJournalSourceCheckpoint, error)
	Save(context.Context, string, CommandJournalSourceCheckpoint) error
}
```

`Save` must make the checkpoint durable before returning. It runs while the
journal persistence barrier holds its lock, so it must not call back into the
journal or wait for work that needs the same journal. The checkpoint contains
the source's opaque binary offset and the latest fully applied journal
sequence. The offset is limited to 1 MiB to bound memory use from malformed or
untrusted input.

## Ingestion Pattern

Create one coordinator for the journal and checkpoint store, load the source
position after the local snapshot/journal has been restored, apply each source
batch through the journal, and commit the batch's final source offset:

```go
coordinator, err := hatCache.NewCommandJournalSourceCheckpointCoordinator(journal, store)
if err != nil {
	return err
}

checkpoint, err := coordinator.Load(ctx, "orders-eu")
if err != nil {
	return err
}

batch, err := source.Read(ctx, checkpoint.Offset)
if err != nil {
	return err
}
for _, event := range batch.Events {
	response := journal.ExecuteCommand(trie, event.Command)
	if !response.OK {
		return errors.New(response.Message)
	}
}
if _, err := coordinator.Commit(ctx, "orders-eu", batch.LastOffset); err != nil {
	return err
}
```

The source batch must be applied before `Commit`. A successful commit records
the journal sequence observed by `WithPersistenceBarrier`, so a restart can
restore the state and source position as one ordered recovery boundary. If
`Save` fails, the source offset is not advanced and the source batch should be
read again. This is at-least-once ingestion; use the journal's idempotency
control or idempotent source commands when duplicate delivery is possible.

The store may combine the received `JournalSequence` with its own snapshot or
metadata transaction. The coordinator does not pretend that an arbitrary
external store is exactly once: that guarantee belongs to the store's durable
commit protocol.

## Recovery Rules

- Call `Load` only after the local journal/snapshot recovery boundary is ready.
- A checkpoint whose journal sequence is ahead of the local journal is rejected
  instead of allowing a source connector to skip input.
- `Load` and `Commit` copy offset bytes; callers may reuse or mutate their input
  buffers after the call returns.
- Empty offsets are valid for sources whose initial position is represented by
  an empty value.
- Source IDs must be non-empty and stable. Offset bytes are treated as opaque;
  they are not logged or interpreted by the coordinator.
- The API has no background worker and no server or configuration flag. It
  changes no ordinary journal-write path until a connector creates a
  coordinator and commits checkpoints.

## Benchmark And Tradeoff

Five benchmark samples on an AMD Ryzen 9 5950X, `linux/amd64`:

| Path | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Existing no-op persistence barrier | 5.463 | 0 | 0 |
| Source checkpoint commit, 3-byte binary offset, no-op store | 31.67 | 8 | 1 |

The opt-in commit is `5.80x` the CPU time of an empty barrier and adds one
8-byte allocation for the ownership copy. A real durable store will dominate
these numbers. Ordinary journal writes and callers that do not use the
coordinator remain unchanged. Raw samples are in
[BENCHMARK.md](BENCHMARK.md#mz-013-source-connector-checkpoints).

Run the focused test and benchmark with:

```sh
make test-mz013-source
make benchmark-mz013-source
```
