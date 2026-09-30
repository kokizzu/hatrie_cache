# T042c Recovery Replay Scalar Batching

Recovery still replays every unsupported or stateful command serially. For
contiguous journal runs of expiry-free `SET` and `SETINT` records, replay now
uses the existing native scalar batch executor instead of taking the trie lock
and building a public response for every record.

The batch is bounded at 256 records. Journal order is unchanged inside each
batch, and a command-family change, TTL/expiry field, idempotency key, local
partition configuration, unsupported command, or batch boundary falls back to
the existing per-record replay path. The normal transaction read barrier is
held while a scalar batch is applied.

This is enabled by default for eligible replay records. It does not reorder
records and it does not implement the rejected parallel replay proposal in
T042.

## Verification

The focused tests cover final values, sequence advancement, contiguous batch
selection, command-boundary fallback, and race detection:

```text
make test-t042-recovery-replay
make race-t042-recovery-replay
make vet-t042-recovery-replay
```

The full `hatCache` package run also exercised the backup and restore tests;
the replay and backup tests passed. The package run still has unrelated
pre-existing SQL expectation failures in this checkout.
