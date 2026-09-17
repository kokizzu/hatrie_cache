# M-U22 Connector Transaction Retry Journal

`hatSql.SQLConnectorTransactionRetryJournal` is an opt-in bounded record of
source transaction intent, attempt number, and sanitized completion outcome.
It closes the gap between a source high-watermark and the connector operation
that was supposed to advance it. Existing `SQLSourceOffsetTracker` and
`SQLSourceTransactionEnvelope` callers are unchanged unless they explicitly
construct a journal.

## Defaults And Limits

`NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{})`
uses a capacity of 1,024 records. The capacity may be set from 1 through
65,536. It is not enabled globally and it does not start a server, background
goroutine, or monitoring endpoint.

The journal retains the oldest records first. A committed or aborted record is
eligible for eviction when the capacity is full. Pending and retryable records
are never evicted; `Begin` returns
`ErrSQLConnectorTransactionRetryJournalCapacity` instead. `Stats().Dropped`
counts terminal records removed by this policy.

## Lifecycle

Call `Begin` before applying the source transaction, and call `Complete` after
the connector knows the outcome:

```go
journal, err := hatSql.NewSQLConnectorTransactionRetryJournal(
	 hatSql.SQLConnectorTransactionRetryJournalOptions{Capacity: 4096},
)
if err != nil {
	 panic(err)
}

envelope := hatSql.SQLSourceTransactionEnvelope{
	 Source: "orders",
	 Transaction: hatSql.SQLSourceTransaction{
		 ID: "orders-00042",
		 Offsets: []hatSql.SQLSourceOffset{{
			 Source: "orders", Partition: "0", Offset: 42,
		 }},
	 },
	 Relations: []string{"orders", "order_items"},
}

record, result, err := journal.Begin(envelope)
// result == hatSql.SQLConnectorTransactionStarted,
// record.State == hatSql.SQLConnectorTransactionPending,
// record.Attempts == 1.
if err != nil {
	panic(err)
}

// Apply all relation changes here. The journal does not replace the caller's
// database transaction or make this callback atomic with the journal itself.
record, err = journal.Complete(hatSql.SQLConnectorTransactionCompletion{
	 Source: "orders", TransactionID: "orders-00042",
	 Attempt: record.Attempts,
	 Outcome: hatSql.SQLConnectorTransactionCommitted,
})
```

The normal result sequence is:

| Call | Result/state | Meaning |
|---|---|---|
| first `Begin` | `started` / `pending`, attempt 1 | intent was recorded before application |
| duplicate `Begin` | `in_progress` / `pending` | do not apply the same attempt twice |
| retryable `Complete` | `retryable` | caller may retry after resolving the connector failure |
| next `Begin` | `retry_started` / `pending`, attempt 2 | a new fenced attempt is open |
| committed `Complete` | `committed` | later duplicate begins return `already_committed` |
| aborted `Complete` | `aborted` | later duplicate begins return `already_aborted` |

`Complete` accepts only `retryable`, `committed`, or `aborted` outcomes. A
completion with an old attempt number returns
`ErrSQLConnectorTransactionRetryJournalConflict`. Repeating an identical
terminal completion is idempotent. A retryable or aborted completion stores
only a safe `ErrorCode` made from ASCII letters, digits, `_`, `-`, and `.`;
raw error messages, URLs, SQL, credentials, and stack traces are rejected.

## Recovery And Checkpointing

The caller owns the durable ordering between the journal checkpoint and the
actual data store. A recovery loop should:

1. Restore the journal snapshot before consuming new source records.
2. Look up the transaction by source and transaction ID.
3. Treat `committed` as already applied and advance the source consumer.
4. Treat `pending` as ambiguous; reconcile the external transaction before
   deciding whether to abort or retry.
5. Treat `retryable` as eligible for a new `Begin`, which increments the
   attempt fence.
6. Treat `aborted` as permanently rejected unless the application creates a
   new source transaction ID.

`Snapshot` and `Restore` are deterministic and defensive. A failed restore is
validated before it replaces any live state. `MarshalBinary` and
`UnmarshalSQLConnectorTransactionRetryJournal` provide the compact default
checkpoint format: a versioned record stream with varints, state bytes, UTC
timestamps, and a CRC-32 checksum. The exported snapshot structs can also be
encoded with the caller's existing JSON tooling when human-readable archival
is more important than wire size.

The binary format contains source metadata, offsets, relation names, state,
attempts, timestamps, and bounded error categories. It does not contain raw
error text. Corrupt, truncated, oversized, invalid-state, duplicate, and
out-of-order snapshots are rejected.

## Costs

The journal is intentionally not wired into the legacy offset fast path. It
adds a map record, lifecycle locking, defensive copies, and retention state.
The measured cost of the complete `Begin` plus `Complete` path is recorded in
[BENCHMARK.md](BENCHMARK.md#mu-022-connector-transaction-retry-journal).
Use it when replay ambiguity and auditability justify the measured CPU and
allocation cost; use the existing offset tracker when only a high-watermark is
needed.
