# MZ-011 Sink Connectors

Status: partially adopted.

MZ-011 adds an importable, bounded sink runner on top of the MZ-010 command
journal subscription. It gives file, HTTP, Kafka, or application-specific
connectors one common delivery and checkpoint lifecycle without adding a
network listener or credentials to `hatrie_cache`.

## Basic Usage

```go
sink := hatCache.CommandJournalSinkFunc(func(ctx context.Context, records []hatCache.CommandJournalRecord) error {
	return publishToRemote(ctx, records)
})

runner, err := journal.StartCommandJournalSink(ctx, sink, hatCache.CommandJournalSinkOptions{
		BatchSize:       64,
		BatchWait:       5 * time.Millisecond,
		CheckpointStore: checkpoints,
})
if err != nil {
	return err
}
defer runner.Close()

if err := runner.Wait(); err != nil {
	return err
}
```

`CommandJournalSink.Write` must finish using the supplied batch before it
returns and must not retain the slice. `CommandJournalSinkFunc` is a small
adapter for function-based connectors.

## Checkpoint Contract

`CommandJournalSinkCheckpointStore.Load` runs before the journal subscription is
registered. Its returned sequence supersedes `AfterSequence`, and replay starts
after that sequence. `Save` runs only after `Write` accepts the entire batch and
receives the last sequence in that batch.

The contract is at least once, not exactly once. If the sink accepts a batch but
checkpoint persistence fails, a restart replays the batch. Connectors should
use an idempotency key based on the journal sequence or implement their own
transactional publish-plus-checkpoint protocol. MZ-012 remains open because a
generic runner cannot make arbitrary external sinks transactional.

## Batching And Backpressure

The default batch size is `64`, the maximum is `65536`, and the default batch
wait is `5ms`. A batch is flushed immediately when it reaches its size; the
wait bounds latency for smaller batches. Subscription replay and live records
share the same batch path, so a connector sees one ordered sequence.

The underlying subscription remains bounded. A slow connector can cause the
subscription to stop with `ErrCommandJournalSubscriptionOverflow`; this is
preferable to unbounded process memory. A connector should return promptly when
its context is canceled. `Close` cancels the runner and waits for its delivery
loop to finish; it returns an earlier sink, checkpoint, or journal error if one
already occurred.

## Failure And Security Rules

- Treat a successful `Write` as acceptance of a batch, not proof of an external
  durable commit unless the connector provides that guarantee.
- Persist checkpoints atomically and monotonically in the connector's storage.
- Keep retry and dead-letter policy in the connector because retry safety is
  sink-specific.
- Journal records may contain keys, values, and internal replication commands.
  Authenticate the owning service before passing records to a connector and do
  not expose raw records to untrusted clients.
- The runner does not open arbitrary sockets, spawn processes, or load plugin
  code. Connectors own those decisions and their configuration.

## Measurements

The raw five-sample comparison is in
[BENCHMARK.md](BENCHMARK.md#mz-011-sink-connectors). On the 100-record replay
fixture, the direct subscription baseline had a median of `88,612 ns/op`,
`69,702 B/op`, and `520 allocs/op`. The sink runner had a median of
`117,787 ns/op`, `142,143 B/op`, and `535 allocs/op`; the checkpoint-enabled
in-memory control was `114,467 ns/op`, `142,215 B/op`, and `535 allocs/op`.
That is approximately `1.33x` CPU, `2.04x` transient bytes, and 15 extra
allocations for the new batching and lifecycle contract. The overhead is opt-in
and does not affect ordinary journal writes or direct subscriptions.

An optimization that drains already-buffered records before creating the batch
timer reduced the runner control from an earlier `124,981 ns/op`, `142,415
B/op`, and `538 allocs/op` to the current `117,787 ns/op`, `142,143 B/op`, and
`535 allocs/op`, approximately `1.06x` faster with three fewer allocations.
