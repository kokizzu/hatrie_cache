# Asynchronous Command Submission

`CommandJournal.SubmitAsyncCommand` is an opt-in, ClickHouse-style asynchronous
write admission path for embedded callers. It reuses the existing bounded group
commit worker and journal format; it does not change the synchronous
`ExecuteCommand` path or any default configuration.

## Usage

```go
journal, err := hatCache.OpenCommandJournalWithOptions(path, hatCache.CommandJournalOptions{
	GroupCommitWindow:   time.Millisecond,
	GroupCommitMaxBatch: 64,
})
if err != nil {
	return err
}
defer journal.Close()

submission, err := journal.SubmitAsyncCommand(trie, hatCache.CacheCommandRequest{
	Command: "SET",
	Key:     "user:42",
	Value:   "active",
})
if errors.Is(err, hatCache.ErrCommandJournalAsyncQueueFull) {
	// Apply caller backpressure or retry with a bounded policy.
}
if err != nil {
	return err
}

response, err := submission.Wait(ctx)
if err != nil {
	return err
}
if !response.OK {
	return errors.New(response.Message)
}
```

## Contract

- The feature is used only when the caller invokes `SubmitAsyncCommand`.
- The journal must have `GroupCommitMaxBatch > 1`; `OpenCommandJournal` already
  uses the default bounded group-commit worker. A journal opened with batch size
  `1` returns `ErrCommandJournalAsyncUnsupported`.
- Only commands that the existing journal would persist are accepted. Reads and
  other non-journaled commands return `ErrCommandJournalAsyncWriteOnly`.
- Successful admission means the request is owned by the queue, not that it is
  durable or visible yet. The queue is bounded by `GroupCommitMaxBatch`; a full
  queue returns `ErrCommandJournalAsyncQueueFull` and never drops an accepted
  request.
- `Status` is `pending` until the worker has synced the journal and applied the
  command, then becomes `completed`. `Done` closes at the same point.
- `Wait(ctx)` returns a cloned response and may be called repeatedly. A command
  rejection is returned as `response.OK == false`; context cancellation only
  stops waiting and does not cancel an accepted write.
- `Close` stops new admission and drains already accepted jobs before returning.
- Requests are copied at admission, including nested batches, mutable JSON-like
  values, binary values, and optional scalar pointers, so callers may reuse
  their request storage after `SubmitAsyncCommand` returns.

`SubmitAsyncCommand` is intentionally embedded-only in this change. HTTP and
gRPC handlers continue to use their existing synchronous response contract; an
external async protocol would need a durable receipt registry and an
authenticated status endpoint, rather than exposing an in-memory future ID.

## Recovery And Safety

The journal record is written and synced before the command is applied, using
the same rollback and idempotency handling as synchronous group commit. Accepted
commands that complete successfully are therefore replayable after restart.
Callers that require a durable acknowledgement must call `Wait` and verify
`response.OK`; returning from `SubmitAsyncCommand` alone is not a durability
acknowledgement. Journal files remain sensitive because they can contain command
keys and values, and the existing file permissions and authentication controls
are unchanged.

## Measured Tradeoff

Command: `make benchmark-async-command` (five samples, Linux, AMD Ryzen 9
5950X, `GroupCommitMaxBatch=64`, `-cpu=4` parallel benchmark workers).

Raw five-sample output:

| Path | Time (ns/op) | Heap (B/op) | Allocs/op |
| --- | --- | ---: | ---: |
| Existing synchronous execute | 231,566; 226,339; 425,614; 416,150; 225,518 | 914; 914; 917; 918; 914 | 3; 3; 3; 3; 3 |
| Async submit and wait | 231,864; 242,231; 239,027; 235,743; 245,418 | 949; 948; 951; 955; 960 | 3; 3; 3; 3; 3 |
| Async admission, completion drained separately | 207,472; 15,369; 16,144; 15,228; 15,892 | 805; 821; 806; 806; 806 | 3; 3; 3; 3; 3 |

Median summary:

| Path | Median time | Median heap | Median allocations | Versus sync time |
| --- | ---: | ---: | ---: | ---: |
| Existing synchronous execute | 231,566 ns | 914 B | 3 | 1.00x |
| Async submit and wait | 239,027 ns | 951 B | 3 | 1.03x |
| Async admission, completion drained separately | 15,892 ns | 806 B | 3 | 0.07x |

The admission figure is about 14.57x lower caller-side latency, but it measures
queue admission and handoff only; completion is deliberately drained outside
the timed region. It therefore must not be presented as 14.57x faster durable
storage. End-to-end async submit-plus-wait is about 1.03x slower than the
existing path in this filesystem run, with about 4.0% more transient heap. This
is a latency-decoupling and batching feature, not a free fsync speedup.

See [BENCHMARK.md](BENCHMARK.md) for the repository-wide raw benchmark record.
