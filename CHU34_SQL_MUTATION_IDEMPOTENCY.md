# CH-U34: Idempotent SQL Mutation Retries

`POST /api/sql` remains read-only unless the request includes a non-empty
`mutation_id`. With a mutation ID, the server executes the supported SQL
mutation through the command journal and makes the retry token durable across
client retries and journal replay.

## Request

```http
POST /api/sql
Content-Type: application/json

{"query":"INSERT INTO cache (key, value) VALUES ('orders:42', 'paid')","mutation_id":"orders-42-payment-v1"}
```

Retry the same JSON query, parameters, and `mutation_id` after a timeout. A
successful duplicate returns HTTP 200 with the same affected-row result and
does not apply the write again. Reusing the ID with a different mutation is a
conflict and returns HTTP 400.

The ID is trimmed and limited to
`MaxCommandJournalIdempotencyKeyBytes` (currently 256 bytes). It is a logical
retry token, not a secret; do not put credentials or personal data in it.

## Configuration

Idempotent HTTP mutations require a journal with a positive bounded
`IdempotencyCapacity`:

```go
journal, err := hatCache.OpenCommandJournalWithOptions(journalPath, hatCache.CommandJournalOptions{
	IdempotencyCapacity: 4096,
})
if err != nil {
	return err
}
defer journal.Close()

handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
	Journal: journal,
}).Handler()
```

`IdempotencyCapacity: 0` disables the journal ledger. The monitoring handler
then rejects mutation requests rather than silently executing them without
retry protection. The capacity is an in-memory bounded lookup plus durable
journal fingerprints; select a value that covers the maximum retry window and
expected concurrent mutations. Eviction ends the protection window for old
tokens, so capacity is not an infinite exactly-once guarantee.

## Semantics

1. The server parses the SQL, builds the journalable command or atomic
   `INSERT ... SELECT` batch, and derives a fingerprint that excludes the
   retry token itself.
2. The command is durably appended before it is applied to the trie.
3. A same-token, same-fingerprint retry returns the recorded successful
   mutation result without applying another write.
4. A same-token, different-fingerprint request fails closed with a conflict.
5. On restart, journal replay applies idempotent entries and refreshes the
   recovered response record before the HTTP server accepts retries.

The direct embedded API remains available as
`ExecuteSQLMutationIdempotent`. The HTTP endpoint is an adapter over that same
path, so the retention and supported-statement limits are shared.

The current idempotent SQL subset is intentionally bounded: journalable direct
cache mutations and `INSERT ... SELECT` batches are supported. Automatic
triggers, `RETURNING`, `ON CONFLICT`, and `MERGE` are rejected until their
result and conditional semantics can be represented without ambiguity. SQL
streaming, cursors, and pagination cannot be combined with mutation requests.

## Authorization And Safety

- Existing monitoring authentication, rate limits, maintenance read-only mode,
  and audit handling still run for the endpoint.
- RBAC source extraction understands `INSERT ... SELECT`; authorization is
  checked before the journaled write.
- A mutation ID is not copied into audit details or used as a path or file
  name. Query text follows the existing SQL audit policy.
- A missing mutation ID keeps the previous read-only `/api/sql` behavior; an
  `INSERT` without the opt-in field is rejected and does not write.
- A configured `MaintenanceReadOnly` handler returns HTTP 503 before journal
  admission.

## Measurements

Five `-benchmem` samples were run with `make benchmark-chu34` on Linux/amd64,
AMD Ryzen 9 5950X, Go's standard benchmark driver, and a 250 ms sample window.
The timed loop is a warmed duplicate HTTP retry; the first durable mutation is
outside the timer so the result describes the common retry path.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Approx. seconds per 10k |
| --- | --- | ---: | ---: | ---: | ---: |
| Before CH-U34: mutation request was rejected by the read-only SQL parser | 17,459; 19,222; 17,440; 17,573; 17,031 | 17,459 | 15,327 | 98 | 0.175 |
| After CH-U34: accepted durable duplicate retry | 31,076; 35,846; 38,099; 37,755; 37,035 | 37,035 | 26,880 | 134 | 0.370 |

The post-change path is 2.12x the CPU time, 1.75x the transient heap, and
1.37x the allocation count of the old rejected request. That comparison is
deliberately labeled: the baseline did no mutation and returned an error,
while the new path performs a valid journal-backed retry lookup. This is an
opt-in reliability cost; requests without `mutation_id` keep the existing
read-only route.

The focused HTTP test also verifies first-write success, duplicate response,
different-command conflict, missing journal, missing ID, maintenance mode,
trie contents, and close/reopen/replay before a post-restart retry.

## Verification

```text
make test-chu34
make test-chu34-package
make race-chu34
make vet-chu34
make benchmark-chu34
make verify-chu34
```
