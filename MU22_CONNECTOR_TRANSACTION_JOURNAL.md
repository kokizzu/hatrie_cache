# M-U22 Connector Transaction Retry Journal

## What it does

`hatPipeline.ConnectorTransactionJournal` records a bounded transaction intent
and its outcome for a connector. It makes replay decisions explicit when a
source operation fails after the destination may have accepted the write.

The journal is deliberately not a source worker, scheduler, or retry loop.
The caller owns delivery, backoff, and side effects. The journal provides an
idempotent state machine and a generation fence that the caller can persist
and inspect during recovery.

## Basic usage

```go
journal, err := hatPipeline.NewConnectorTransactionJournal(ctx,
	 hatPipeline.ConnectorTransactionJournalOptions{
		Capacity: 256,
		Store:    store, // optional; nil keeps the journal process-local
	})
if err != nil {
	return err
}

intent := hatPipeline.ConnectorTransactionIntent{
	ID:          "orders-000042",
	ConnectorID: "orders",
	Generation:  7,
	Offset:      []byte("source-offset"),
	Frontier:    []byte("source-frontier"),
}

transaction, err := journal.Begin(ctx, intent)
if err != nil {
	return err
}

if err := deliver(transaction); err != nil {
	_, err = journal.Fail(ctx, intent.ID, intent.Generation, err.Error())
	return err
}

_, err = journal.Commit(ctx, intent.ID, intent.Generation)
return err
```

When a caller decides that a failed transaction is safe to try again, it
calls `Retry`. A repeated `Begin` with the same ID, connector, generation,
offset, and frontier returns the existing record without changing its
revision. A different intent using an existing ID is rejected.

## State machine

| State | Allowed transitions | Meaning |
| --- | --- | --- |
| `pending` | `failed`, `committed`, `aborted` | Intent is in flight. |
| `failed` | `pending` via `Retry`, `aborted` | The last attempt did not complete. |
| `committed` | none | Destination completion is recorded. |
| `aborted` | none | The caller permanently stopped the intent. |

`Generation` prevents an old connector worker from changing a newer worker's
transaction. Terminal records are retained until capacity is needed. Pending
or failed records are never evicted, so a full journal returns
`ErrConnectorTransactionJournalFull` instead of silently losing work.

## Persistence

`ConnectorTransactionJournalStore` has the same `Load`/`Save` shape as the
existing connector checkpoint store, so an existing atomic file store can be
adapted without a new storage service. The journal writes a complete `HCT1`
image after each mutation when a store is configured. A failed save restores
the in-memory state and returns the store error.

`HCT1` uses big-endian fixed-width counters, length-prefixed bounded fields,
and CRC32C. The format is bounded by:

- 1 to 4096 retained records, with a default capacity of 256;
- 256 bytes for a transaction ID and connector ID;
- 512 KiB each for offset and frontier data;
- 4096 bytes for the last error;
- 1 MiB for the complete encoded snapshot.

The format is versioned and rejects malformed lengths, invalid states,
duplicate IDs, invalid generations, and checksum failures before allocating
unbounded memory. The store is responsible for atomic replacement and durable
flush semantics; the journal does not claim an `fsync` that the store does not
provide.

## Benchmark

The benchmark ran on the repository's AMD Ryzen 9 5950X host with
`-benchtime=200ms -count=5`. The table reports the median of the five runs.

| Operation | Workload | ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| `Begin` + `Commit` | 256-record in-memory journal | 2,060 | 0 | 0 |
| `Fail` + `Retry` + `Commit` | 1-record in-memory journal | 187 | 0 | 0 |
| `EncodeConnectorTransactionSnapshot` | 64 records | 8,166 | 21,360 | 8 |
| `DecodeConnectorTransactionSnapshot` | 64 records | 13,142 | 18,088 | 324 |

For context, the existing single-record `HCP1` checkpoint baseline measured
44.72 ns/op, 64 B/op, and 1 allocation for encode, and 72.92 ns/op, 40 B/op,
and 3 allocations for decode. These are different workloads and are not
claimed as a speedup: `HCT1` carries a bounded transaction journal rather than
one checkpoint. The important hot-path result is that a process-local journal
does not allocate per state transition. With a durable store configured, each
mutation pays the snapshot encoding and store write cost in exchange for
recovery visibility.

## Operational guidance

Use a capacity large enough to cover the maximum number of concurrently
pending or retryable transactions. A capacity that is too small intentionally
fails closed when every retained record is non-terminal. Keep IDs stable
across process restarts and include the connector generation in the worker
fencing scheme. Treat `LastError` as operator-visible data and avoid placing
secrets in it.
