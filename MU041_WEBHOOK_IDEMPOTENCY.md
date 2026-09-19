# M-U41 Webhook Event Idempotency

This adds an opt-in bounded event-ID ledger for Materialize-style external
event ingestion. A source and event ID identify one logical event; the ledger
stores only a SHA-256 payload fingerprint, so a retry with the same body is a
duplicate while reuse of the ID with a different body is rejected.

```go
ledger, err := hatSql.NewWebhookEventDeduplicator(hatSql.WebhookEventDeduplicatorOptions{
	Capacity: 4096,
	TTL:      24 * time.Hour,
})
if err != nil {
	return err
}

decision, err := ledger.Accept("payments", eventID, requestBody, receivedAt.UTC())
switch {
case err != nil:
	return err
case decision == hatSql.WebhookEventDuplicate:
	return nil // acknowledge an identical retry without applying it again
default:
	return applyEvent(requestBody)
}
```

## Contract

- `Accept` is atomic per source/event-ID pair. The first body is accepted,
  identical live retries return `WebhookEventDuplicate`, and conflicting live
  bodies return `ErrWebhookEventConflict`.
- Entries expire after the configured TTL. `Prune` removes expired entries
  explicitly; a full ledger also prunes before returning capacity exhaustion.
  Live entries are never silently evicted.
- Source names, event IDs, payload bytes, capacity, and snapshot bytes have
  explicit bounds. Payload bytes are hashed and discarded rather than retained.
- `Snapshot` emits deterministic, CRC32C-protected `HWE1` bytes containing
  sorted source/event IDs, expiry timestamps, and fingerprints. `Restore`
  validates the entire snapshot and replaces the ledger only after successful
  validation; expired records are discarded relative to the supplied time.
- Zero options select a 4,096-entry capacity, 24-hour TTL, 128-byte source
  names, 256-byte event IDs, 16 MiB payloads, and 8 MiB snapshots.
- The ledger is not automatically attached to `WebhookSink` or an HTTP route.
  The caller owns event application, durable snapshot storage, permissions,
  retry acknowledgement, and connector integration. Existing behavior stays
  unchanged unless a caller creates the ledger.

## Measured Cost

Five samples on an AMD Ryzen 9 5950X using `go test -benchmem`:

| Operation | Median | Memory | Comparison |
|---|---:|---:|---:|
| SHA-256 payload fingerprint | 101.4 ns/op | 0 B/op, 0 allocs/op | Baseline |
| Duplicate `Accept` | 147.1 ns/op | 0 B/op, 0 allocs/op | 1.45x CPU, 0 allocations |
| Snapshot of 1,024 events | 249,419 ns/op | 106,600 B/op, 5 allocs/op | Explicit persistence cost |

The duplicate path adds a bounded map lookup and mutex around the existing
fingerprint work. It is a deliberate opt-in correctness cost, not a faster
replacement for hashing; the default path pays nothing.

## Verification

```text
make test-mu41
make benchmark-mu41-baseline
make benchmark-mu41
make verify-mu41
```

Tests cover replay, payload conflicts, expiry, capacity behavior, deterministic
snapshot/restore, CRC failure atomicity, validation bounds, and concurrent
same-event retries.
