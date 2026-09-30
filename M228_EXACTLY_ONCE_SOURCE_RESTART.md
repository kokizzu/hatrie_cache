# M228 Exactly-Once Source Restart

M228 closes the crash window between applying a source transaction to the
command journal and saving the source connector's offset. It is an opt-in API;
ordinary commands, the monitoring server, and the default journal path are
unchanged.

## Usage

Open the journal with bounded durable idempotency enabled, then create one
coordinator for the source checkpoint store:

```go
journal, err := hatCache.OpenCommandJournalWithOptions("data/commands.journal", hatCache.CommandJournalOptions{
	GroupCommitMaxBatch: 1,
	IdempotencyCapacity: 1024,
})
if err != nil {
	return err
}

coordinator, err := hatCache.NewCommandJournalExactlyOnceSourceCoordinator(journal, checkpointStore)
if err != nil {
	return err
}

result, err := coordinator.ApplyBatch(ctx, trie, hatCache.CommandJournalExactlyOnceSourceBatch{
	SourceID:      "orders-eu",
	TransactionID: "kafka-orders-eu-00000042",
	Offset:        []byte{0, 1, 255},
	Commands: []hatCache.CacheCommandRequest{
		{Command: "SETINT", Key: "orders:42", Value: "1"},
	},
})
if err != nil {
	return err
}
if result.AlreadyCommitted {
	// The same transaction was already durably applied and checkpointed.
}
```

`CommandJournalSourceCheckpointStore.Save` must finish its own durable write
before returning and must not call back into the journal. The store receives
the source ID, opaque offset, transaction ID, batch fingerprint, and the
journal sequence observed by the persistence barrier.

## Recovery Contract

1. Restore the local snapshot and command journal before creating the source
   coordinator.
2. Read the source position from the checkpoint store.
3. Read a source transaction at that position and call `ApplyBatch`.
4. On a checkpoint-store failure, retry the exact same transaction ID, offset,
   and commands.
5. Treat a changed offset or command list under the same transaction ID as a
   conflict and investigate the source connector rather than applying it.

The first call writes one atomic journal `BATCH`. If command execution fails,
the atomic batch is rolled back and no source checkpoint is saved. If command
execution succeeds but `Save` fails, the batch remains durable and the retry
uses its deterministic idempotency key. `AlreadyCommitted` is returned only
after the checkpoint itself has been durably loaded and matches the retry.

## Bounds And Security

- `SourceID` must be non-empty.
- Transaction IDs are trimmed, NUL-free, and limited to 256 bytes.
- Offsets are opaque and limited to 1 MiB.
- A source batch must contain at least one and at most the public batch limit
  of journalable commands.
- Nested `BATCH` commands and internal replication commands are rejected.
- Caller command slices, values, pairs, and pointer fields are deep-copied
  before execution and fingerprinting.
- Idempotency is bounded. Configure capacity and retention for the maximum
  checkpoint-save retry window; this API does not provide unbounded deduplication.
- The coordinator serializes its own calls. A source should use one coordinator
  per journal/source pair rather than sharing it across unrelated sources.

The API does not expose transaction IDs, offsets, or command values through a
new monitoring endpoint. Existing journal encryption and authentication
settings remain responsible for protecting journal data at rest and in
transit.

## Binary Compatibility

Binary command-journal payload version 5 stores `Atomic` and the nested batch
payload. Existing binary payload versions 1 through 4 remain readable. A
binary journal tail containing a batch uses tail envelope version 3; scalar
tails continue using the existing version-2 envelope. JSON journal format is
unchanged as a fallback.

## Measurement

Five samples on `linux/amd64`, AMD Ryzen 9 5950X:

| Path | Median ns/op | Median B/op | Median allocs/op | Relative to historical legacy control |
| --- | ---: | ---: | ---: | ---: |
| M227 legacy command plus `Commit` | 672,634 | 290 | 4 | 1.00x |
| M228 legacy control on feature branch | 670,959 | 298 | 5 | 1.00x CPU |
| M228 exactly-once `ApplyBatch` | 699,244 | 3,240 | 39 | 1.04x CPU, 11.17x bytes, 9.75x allocations |
| M228 duplicate retry short-circuit | 1,727 | 1,033 | 13 | 389.50x lower CPU than full legacy write |
| M228 binary batch-tail encode | 336.5 | 200 | 3 | 77 wire bytes |

At 10,000 operations, the measured full paths are approximately 6.73 seconds
for the legacy control and 6.99 seconds for exactly-once `ApplyBatch`; 10,000
recognized duplicate retries take approximately 0.017 seconds.

This is a correctness feature, not a hot-path optimization: a fresh
exactly-once transaction pays for fingerprinting, deep input ownership, the
atomic batch envelope, and the checkpoint load/save. The retry path is cheap
because it does no second mutation or journal append. Raw samples are in
[`M228_BENCHMARK_RAW.txt`](M228_BENCHMARK_RAW.txt), and the consolidated
benchmark index is in [`BENCHMARK.md`](BENCHMARK.md#m228-exactly-once-source-restart).

Run the focused regression and benchmark with:

```sh
make test-m228
make benchmark-m228
```
