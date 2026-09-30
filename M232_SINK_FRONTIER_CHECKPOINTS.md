# M232 Sink Frontier Checkpoints

M232 adds an opt-in checkpoint coordinator that couples a sink acknowledgement
to the exact progress message that was emitted. A numeric frontier alone cannot
prove which subscription revision or stream instance the sink observed after a
restart.

## Use

```go
message, err := hatSql.NewSQLSinkFrontierMessageFromBatch(
	"orders", "region-a", batch,
)
if err != nil {
	return err
}

emitted, err := coordinator.Emit(message)
if err != nil {
	return err
}

// Send message to the external sink, then acknowledge the same value only
// after the sink confirms that exact message.
acknowledged, err := coordinator.Acknowledge(message)
if err != nil {
	return err
}
_ = emitted
_ = acknowledged

snapshot := coordinator.Snapshot()
// Persist snapshot with the application's atomic, durable checkpoint store.
if err := restored.Restore(snapshot); err != nil {
	return err
}
```

`NewSQLSinkFrontierMessageFromBatch` accepts only progress-only
`QuerySubscriptionDeltaBatch` values. The batch must have a non-zero
subscription ID and revision, and must not contain columns, row deltas, or a
reset marker. This keeps the checkpoint identity small and prevents a data
batch from being mistaken for a frontier message.

## Semantics

- `Emit` stores the exact sink, partition, subscription ID, revision, frontier,
  and completion bit.
- Re-emitting the same message is an idempotent no-op.
- A newer frontier replaces the previous message and starts unacknowledged.
- A different message at the same frontier is a conflict; an older frontier is
  stale.
- `Acknowledge` succeeds only for the currently emitted message. A message from
  a superseded revision or stream cannot advance the checkpoint.
- Repeated acknowledgement of the same exact message is an idempotent no-op.
- `Snapshot` is deterministic. `Restore` validates the complete input before
  replacing state, so invalid or duplicate entries do not partially apply.
- Retention is bounded to `MaxSQLSinkFrontierCheckpointEntries` (65,536), and
  sink and partition names are bounded to
  `MaxSQLSinkFrontierMessageNameBytes` (256 bytes).

The coordinator retains metadata only; it does not retain row payloads or
perform durable I/O. The caller must atomically and durably save the snapshot
before considering the checkpoint committed. The existing
`SQLSinkProgressTracker` and `SQLSinkTwoPhaseCoordinator` APIs are unchanged.

## Cost

The microbenchmark used five 200 ms samples on the local AMD Ryzen 9 5950X.
It measures coordinator CPU and heap behavior only; network and durable-store
latency are excluded.

| Operation | Median | Memory | Relative CPU |
| --- | ---: | ---: | ---: |
| Existing numeric progress acknowledgement | 58.78 ns/op | 0 B/op, 0 allocs/op | 1.00x |
| Exact frontier `Emit` + `Acknowledge` | 134.4 ns/op | 0 B/op, 0 allocs/op | 2.29x |
| Snapshot of 64 partitions | 8,971 ns/op | 5,016 B/op, 4 allocs/op | snapshot-only |

The tradeoff is intentional: the exact-message path costs about 75.6 ns per
emit/ack pair in this in-process benchmark in exchange for retaining stream
identity and rejecting stale acknowledgements after a crash or reconnect.
Snapshot cost is paid only when the caller persists a checkpoint.

Run the focused correctness and race checks with:

```text
make test-m232
make race-m232
make benchmark-m232
```

Raw samples are in [`M232_BENCHMARK_RAW.txt`](M232_BENCHMARK_RAW.txt), and the
consolidated comparison is in
[`BENCHMARK.md`](BENCHMARK.md#m232-sink-frontier-checkpoints).
