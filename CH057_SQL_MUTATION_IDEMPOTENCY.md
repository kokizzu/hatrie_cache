# CH-057: Journal-Backed SQL Mutation Idempotency

This is a ClickHouse-inspired insert-deduplication token for the SQL mutation
API. It is opt-in because it retains a bounded token index and requires a
durable command journal.

## Usage

Open the journal with a positive idempotency capacity, then use the explicit
API for mutations that may be retried by a client:

```go
journal, err := hatCache.OpenCommandJournalWithOptions(path, hatCache.CommandJournalOptions{
	IdempotencyCapacity: 4096,
	GroupCommitMaxBatch: 1,
})
if err != nil {
	return err
}
defer journal.Close()

result, err := hatCache.ExecuteSQLMutationIdempotent(
	ctx,
	journal,
	trie,
	"INSERT INTO cache (key, value) VALUES ('order:42', 'accepted')",
	nil,
	hatCache.SQLQueryOptions{},
	"orders-2026-09-14-42",
)
```

Retrying the same statement with the same token returns a successful response
with the same mutation result, without applying or appending the mutation
again. After replay, the response message may be the journal's generic
recovered-idempotency message, but no write is repeated. Reusing a token for a
different generated command returns an error. The token is trimmed and limited
to `MaxCommandJournalIdempotencyKeyBytes` (`256`) bytes.

`INSERT ... SELECT` is also supported. The generated atomic `BATCH` receives
one token, so the complete selected batch is deduplicated as one operation.
The source query must still be available when a client retries it; the batch
fingerprint protects against a changed selected result.

## Boundaries

The API rejects `RETURNING`, `ON CONFLICT`, `MERGE`, and automatic trigger
execution. Those paths have result or conditional semantics that are not
encoded by the existing public journal command record. The ordinary
`ExecuteSQLMutation` API remains unchanged and does not allocate or hash an
idempotency token.

`IdempotencyCapacity` bounds retained in-memory records. The journal remains
the durable source of truth: after restart, open it with a positive capacity
and replay it into the trie before accepting retries. A zero-capacity journal
is rejected by the idempotent API instead of silently providing at-most-once
behavior.

## Measurement

Measured with `make benchmark-sql-mutation-idempotency` on Linux/amd64 using
an AMD Ryzen 9 5950X. The benchmark repeats one SQL insert after setup for the
retry cases and uses five samples:

| Path | Raw `ns/op` samples | Median | Heap | Allocs |
| --- | --- | ---: | ---: | ---: |
| Direct SQL mutation, no journal | 6581; 6525; 6474; 6427; 6831 | 6525 | 8960 B/op | 29 |
| Journaled mutation, no token | 674168; 1196789; 676083; 1909708; 750631 | 750631 | 259-265 B/op | 2 |
| Idempotent SQL retry | 7520; 7424; 7653; 7364; 7584 | 7520 | 9237 B/op | 31 |

The idempotent retry path is `1.15x` the direct no-journal latency, with
`1.03x` the measured heap and two additional allocations. That is the explicit
cost of parsing/fingerprinting and the bounded journal lookup. The journaled
no-token path is dominated by synchronous durable writes and is intentionally
shown separately; its noisy median is about `0.75 ms/op` on this filesystem.
The feature's benefit is correctness and duplicate-write suppression, not a
faster first write. The default API remains the lower-cost path when retries
are not required.
