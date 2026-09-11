# MZ-012 Exactly-Once Sink Checkpoints

Status: adopted as a contractual, sink-owned transaction API.

MZ-012 extends the MZ-011 sink runner for connectors that can atomically commit
their output and the journal sequence watermark. The API does not pretend that
an arbitrary HTTP request or message broker call is transactional; exactly-once
behavior exists only when the supplied sink implements the contract below.

## Transactional API

```go
runner, err := journal.StartCommandJournalExactlyOnceSink(ctx, sink, hatCache.CommandJournalExactlyOnceSinkOptions{
	ReplayLimit: 1000,
	BatchSize:   64,
	BatchWait:   5 * time.Millisecond,
})
if err != nil {
	return err
}
defer runner.Close()
return runner.Wait()
```

`CommandJournalExactlyOnceSink` has three responsibilities:

1. `LoadSequence` returns the sink's last atomically committed journal
   sequence.
2. `Begin(ctx, expectedSequence)` starts a transaction against that watermark.
   The sink should reject a stale or conflicting expected sequence.
3. The returned transaction stages the complete batch in `Write`, atomically
   publishes the staged output and its last sequence in `Commit`, and discards
   uncommitted output in `Rollback`.

The runner calls `Write` before `Commit`, advances its local watermark only
after a successful commit, and passes the prior watermark to the next
transaction. A checkpoint is not stored separately from the sink transaction.

## Failure Semantics

- A `Write` failure invokes `Rollback` and stops the runner. No checkpoint
  advances.
- A pre-commit rollback failure is joined with the write error.
- A `Commit` failure is terminal and is not retried automatically. The external
  outcome may be unknown; restarting and calling `LoadSequence` lets the sink
  resolve whether the batch committed before deciding whether replay is needed.
- Context cancellation and explicit `Close` cancel the active operation and do
  not report a cancellation error for an explicit runner close.
- The underlying subscription remains bounded and can report replay,
  compaction, journal-close, or overflow errors.

The guarantee is exactly once only if the sink makes output and watermark
publication one atomic durable operation, makes `LoadSequence` observe that
same operation, and rejects concurrent conflicting transactions. The runner
serializes batches for one sink but does not coordinate independent runners.

Use the MZ-011 `CommandJournalSink` when the external system is at-least-once,
or when connector-specific idempotency is sufficient and a transaction is not
available.

## Limits And Security

Replay, buffer, poll, batch-size, and batch-wait defaults match the bounded
MZ-011 runner. The API adds no listener, network access, credential handling, or
plugin loading. Journal records may contain keys, values, and internal
replication commands, so authentication and authorization remain the owning
service's responsibility.

## Measurements

Raw five-sample results are in
[BENCHMARK.md](BENCHMARK.md#mz-012-exactly-once-sink-checkpoints). On the
100-record replay fixture, the MZ-011 at-least-once runner had a median of
`129,303 ns/op`, `142,146 B/op`, and `535 allocs/op`. The exact-once runner with
an in-memory no-op transaction had `126,302 ns/op`, `141,969 B/op`, and `535
allocs/op`. The difference is measurement noise; real transaction and durable
commit costs are sink-specific and are intentionally not hidden by this API.
