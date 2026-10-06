# CH-U01 Durable Asynchronous-Insert Deduplication

`hatPipeline.NewDurableAsyncBatcher` adds an opt-in durable insert-ID ledger
around the existing `AsyncBatcher`. It is intended for clients that retry an
asynchronous insert after a timeout or process restart and need completed
insert IDs to suppress duplicate sink work.

The existing `NewAsyncBatcher` API and its defaults are unchanged. The durable
wrapper is not enabled implicitly because every committed ID requires a ledger
write, and synchronous durability can require an `fsync`.

## Example

```go
ledger, err := hatPipeline.NewAsyncInsertLedger(hatPipeline.AsyncInsertLedgerOptions{
	Path:       "/var/lib/hatrie/async-insert-ids.hid",
	MaxEntries: 100_000,
	TTL:        24 * time.Hour,
})
if err != nil {
	return err
}
defer ledger.Close()

batcher, err := hatPipeline.NewDurableAsyncBatcher(
	hatPipeline.DurableAsyncBatcherOptions[[]byte]{
		Ledger: ledger,
		Batcher: hatPipeline.AsyncBatcherOptions[hatPipeline.AsyncInsert[[]byte]]{
			Capacity:      1024,
			MaxBatchSize:  64,
			FlushInterval: 10 * time.Millisecond,
		},
		Handler: func(ctx context.Context, batch []hatPipeline.AsyncInsert[[]byte]) error {
			return writeToSink(ctx, batch)
		},
	},
)
if err != nil {
	return err
}

accepted, err := batcher.Submit(ctx, request.InsertID, request.Payload)
if err != nil {
	return err
}
if !accepted {
	// The ID was already committed or is currently in flight.
	return nil
}
return batcher.Flush(ctx)
```

`Close` drains the batcher but does not close the ledger; close the batcher
before closing the ledger.

## Semantics

- `Acquire` reserves an ID in process memory. A duplicate reservation returns
  `false, nil` without invoking the sink.
- IDs are written to the ledger only after the complete sink batch returns nil.
- A failed sink batch releases its reservations so the caller can retry.
- Pending reservations are not durable. After a crash they are replayable,
  which avoids losing an insert accepted before the handler ran.
- A crash after the sink commits but before the ledger commit can still cause a
  retry. Exactly-once behavior across an arbitrary sink requires the sink and
  ledger to share a transaction or the sink to enforce the same insert ID.
- Committed IDs expire after `TTL`. `PurgeExpired` removes expired IDs and
  compacts the journal when its historical record count grows beyond the live
  set.
- `MaxEntries` bounds live committed plus in-flight IDs. The default is
  `100,000`; the maximum is `1,048,576`.
- IDs are limited to 4 KiB and the ledger file is created with mode `0600`.
- The file is a single-writer resource. Do not open one path from multiple
  processes.

## Durability Modes

The zero-value `AsyncInsertLedgerDurabilitySync` mode writes and syncs each
completed batch before the batcher acknowledges it. Use
`AsyncInsertLedgerDurabilityBuffered` only when losing the most recent ledger
records after a process or host crash is acceptable; `Close` still syncs the
file.

The ledger uses a versioned `HID1` record shape with a CRC32C checksum. A
truncated final record is discarded during recovery; a corrupt complete record
before the tail is rejected.

## Measured Tradeoff

Commands:

```text
make codex-chu01-dedupe-test
make codex-chu01-dedupe-race
make codex-chu01-dedupe-vet
make codex-chu01-dedupe-batched-bench
```

Five samples on the AMD Ryzen 9 5950X workstation:

| Workload | Median | Bytes/op | Allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Existing batcher, one submit + flush | 914.7 ns | 128 B | 2 | Baseline |
| Durable ledger, one synchronous submit + flush | 724,192 ns | 367 B | 4 | 791x slower; `24.39` journal bytes/op |
| Durable ledger, buffered submit + flush | 112,638 ns | 344 B | 5 | 123x slower; not crash-durable until sync/close |
| Existing batcher, 64 submits + flush | 10,600 ns/batch | 128 B | 2 | Baseline |
| Durable ledger, 64 submits + one synchronous flush | 9,209,198 ns/batch | 18,034 B | 135 | 869x slower; `25.61` journal bytes/item |
| Already-committed duplicate lookup | 95.39 ns | 0 B | 0 | No sink work or journal write |

The synchronous numbers are dominated by filesystem flush latency and showed
wide local variance, including occasional multi-second single-operation
samples. This is therefore a reliability feature, not a performance
optimization. Use the existing `AsyncBatcher` when retry deduplication is not
required, and batch multiple inserts per durable flush when the durability
contract permits that latency shape.
