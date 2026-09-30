# M231 Exactly-Once Upsert Sinks

The existing transactional journal sink commits a batch and a sequence
watermark together, but its `Write` method is append-shaped. A commit response
can be lost after the destination has durably applied the batch; replay then
needs a destination key that can safely overwrite the same output.

M231 adds an opt-in sink contract with an explicit durable output identity:

```go
type CommandJournalExactlyOnceUpsertSink interface {
    LoadSequence(context.Context) (uint64, error)
    Begin(context.Context, uint64) (CommandJournalExactlyOnceUpsertTransaction, error)
}

type CommandJournalExactlyOnceUpsertTransaction interface {
    Upsert(context.Context, CommandJournalExactlyOnceUpsertRecord) error
    Commit(context.Context, uint64) error
    Rollback(context.Context) error
}
```

Start it with `StartCommandJournalExactlyOnceUpsertSink`. The runner loads the
sink-owned sequence, consumes bounded journal batches, derives and validates
one identity per record, calls `Upsert`, and commits the output plus watermark
in one sink transaction.

## Identity Rules

- A nil identity function uses `journal:<sequence>`, which is deterministic
  and safe for replay.
- A custom identity must be stable for the same logical output across process
  restarts. Prefer a canonical business key such as `tenant:order_id`, not a
  random UUID or a timestamp.
- Identities are trimmed, must be non-empty and NUL-free, and are limited to
  256 bytes.
- Duplicate identities within one batch are rejected before any upsert is
  accepted. Duplicate deliveries in later batches are expected and must be
  handled as overwrite/upsert operations by the destination.

## Recovery Semantics

`Commit` must atomically publish all staged upserts and the last journal
sequence. If `Commit` returns an error, its outcome is unknown and the runner
stops without retrying the same transaction. On restart, `LoadSequence`
causes the journal to replay any batch whose watermark was not durably
advanced. The sink then receives the same `OutputIdentity` and can overwrite
the already-published row instead of appending a duplicate.

This is exactly-once only within the sink's transaction boundary. The API
cannot make an arbitrary external HTTP call or non-transactional database
exactly-once. External adapters must provide a durable idempotent upsert and
an atomic output-plus-watermark commit, or explicitly document an at-least-once
fallback.

## Cost

The benchmark compares the existing `Write(batch)` transaction call with the
new identity preparation plus one `Upsert` call per record. It uses no-op
transactions with an observable counter, so it measures API and allocation
overhead without hiding it behind network or storage latency. Five 200 ms
samples were run on `linux/amd64` with an AMD Ryzen 9 5950X.

| Batch | Existing write | Upsert path | CPU change | Upsert memory |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1.47 ns/op, 0 B/op, 0 allocs/op | 213 ns/op, 272 B/op, 2 allocs/op | 145x | 272 B/batch |
| 64 | 1.53 ns/op, 0 B/op, 0 allocs/op | 11,157 ns/op, 20,904 B/op, 68 allocs/op | 7,292x | 327 B/record |
| 256 | 1.36 ns/op, 0 B/op, 0 allocs/op | 43,738 ns/op, 83,240 B/op, 417 allocs/op | 32,066x | 325 B/record |

The baseline is deliberately a minimal transaction-call control, not a claim
that a real durable write costs 1 ns. The meaningful tradeoff is the new
per-batch identity map and per-record identity strings; real sink I/O will
usually dominate these numbers. The feature is opt-in, so existing sinks pay
no cost. Reproduce with:

```text
make benchmark-m231
```

Raw output is kept in [`M231_BENCHMARK_RAW.txt`](M231_BENCHMARK_RAW.txt).

## Verification

Focused tests cover stable custom identities, the default sequence identity,
commit uncertainty followed by replay, duplicate/unsafe identities, callback
errors, nil transactions, rollback, and bounded batching:

```text
make test-m231
```
