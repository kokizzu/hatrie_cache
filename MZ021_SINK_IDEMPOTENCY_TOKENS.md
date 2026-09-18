# MZ-021 Sink Idempotency Tokens

MZ-021 derives a stable sink idempotency key from the logical delivery
identity instead of from a retry-specific transaction ID. This makes a
recreated sink attempt safe to retry after a timeout or process restart when
the caller supplies the same frontier and exact batch bytes.

## API

```go
commit := hatSql.SQLSinkCommit{
	Sink:          "warehouse",
	TransactionID: "attempt-2026-09-18-001",
	Progress: []hatSql.SQLSinkProgress{
		{Sink: "warehouse", Partition: "0", Frontier: 42},
	},
}

derived, err := hatSql.DeriveSQLSinkCommitIdempotencyKey(commit, batchBytes)
if err != nil {
	return err
}

committed, err := ledger.Commit(derived, func(transactionID string) error {
	return publishToWarehouse(transactionID, batchBytes)
})
```

The lower-level `DeriveSQLSinkIdempotencyKey` accepts
`SQLSinkIdempotencyTokenInput` when the caller only needs the key. The commit
helper returns a detached copy, preserves the transaction ID, clones the
progress slice, and replaces any caller-supplied key with the derived token.

## Canonical identity

The token is the lowercase 64-character SHA-256 digest of a versioned,
length-prefixed encoding of:

1. The domain string `hatrie-cache/sql-sink-idempotency/v1`.
2. The trimmed sink name.
3. The progress entries sorted by partition, with each partition and frontier.
4. The exact batch bytes supplied by the caller.

The transaction ID is intentionally excluded. Progress order therefore does
not matter, but changing a frontier, sink, partition, or batch byte changes
the token. JSON with different whitespace or object-key order is different
input unless the caller canonicalizes it before deriving the token.

The input is rejected when the sink or progress is malformed, a partition is
duplicated, or the bounded limits in `MaxSQLSinkIdempotencyBatchBytes` and
`MaxSQLSinkIdempotencyProgress` are exceeded. Progress entries must belong to
the same sink passed in `Sink`.

## Retry and conflict behavior

Use the same sink, progress, and batch bytes for every retry of one logical
delivery. A retry may generate a new transaction ID: the exactly-once ledger
recognizes the same derived key and progress as a duplicate, so the callback
is not invoked twice. Reusing a key with different progress remains a
conflict. The ledger still rejects reuse of one transaction ID for a different
idempotency key.

The token is an idempotency identifier, not an authentication mechanism. Do
not treat it as proof that a batch came from a trusted producer; authenticate
the transport and authorize the sink operation separately.

## Cost and optimization measurement

The benchmark isolates key assignment/generation rather than external sink
I/O. It runs five 500 ms samples with `-benchmem` on Linux/amd64, AMD Ryzen 9
5950X. The supplied-key row only assigns an already available key, so it is a
lower-bound comparison for callers that already have a durable token.

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Supplied key baseline | 0.462 | 0 | 0 |
| First derived implementation | 703.5 | 464 | 14 |
| Optimized derived implementation | 260.8 | 64 | 1 |

The optimized path is about 2.70x faster than the first implementation, uses
7.25x fewer temporary bytes, and uses 14x fewer allocations. It uses a small
stack buffer and insertion sort for ordinary batches, then streams larger
canonical inputs directly into SHA-256. The remaining cost is the unavoidable
hash and digest-string generation; this is not a claim that deriving a token
is free compared with reusing a token already stored by the source.

Run the focused benchmark with:

```text
make benchmark-mz021-sink-idempotency
```
