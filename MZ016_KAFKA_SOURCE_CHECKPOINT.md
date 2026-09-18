# MZ-016: Durable Kafka Source Checkpoints

`hat/hatSql` already applied Kafka rows, partition offsets, and replay markers
atomically in memory. MZ-016 adds an importable checkpoint-store contract for
making those three pieces durable as one snapshot.

## Protocol

```go
type KafkaTableSourceCheckpointStore interface {
    Load(context.Context, string) (hatSql.KafkaTableSourceSnapshot, bool, error)
    Commit(context.Context, hatSql.KafkaTableSourceSnapshot) error
}
```

`Commit` must atomically replace the complete snapshot in the durable store.
The store must treat a returned error as "not committed"; the source rolls
back its in-memory state when that contract is violated by a failed commit.

Restore before consuming:

```go
found, err := source.RestoreCheckpoint(ctx, store)
if err != nil {
    return err
}
_ = found
```

Apply and persist one batch:

```go
result, err := source.ApplyBatchWithCheckpoint(ctx, batch, store)
```

For a polling consumer, use `ConsumeOnceWithCheckpoint`. It persists the
source snapshot first and calls the broker's `Commit` only afterward:

```go
result, err := source.ConsumeOnceWithCheckpoint(ctx, consumer, store)
```

If the process stops after the durable store commit but before the broker
offset commit, the next process restores the replay marker and re-reading the
batch is a duplicate no-op. If the store commit fails, rows, offsets, replay
markers, and source statistics return to their pre-batch state.

The source lock is held while the store's `Commit` runs to prevent a concurrent
batch from being omitted from or accidentally included in the checkpoint.
Checkpoint stores must therefore avoid calling back into the same source from
inside `Commit`.

## Tradeoff

The existing `ApplyBatch` and `ConsumeOnce` paths remain unchanged and have no
checkpoint overhead. The new methods clone the complete detached source
snapshot, so checkpoint cost grows with retained rows and replay markers. Use
the new path when restart correctness is more important than the extra copy;
use the existing path when an external system already provides equivalent
durability.

Benchmark command: `make benchmark-mz016-kafka-source-checkpoint` on an AMD
Ryzen 9 5950X, one retained row and a no-op checkpoint store:

| Path | Raw ns/op samples | B/op | allocs/op |
| --- | --- | --- | --- |
| Normal `ApplyBatch` | 5002, 4796, 5125, 4738, 4688 | 2800 | 36 |
| `ApplyBatchWithCheckpoint` | 5832, 5846, 5992, 6147, 6727 | 3728 | 49 |

Median checkpoint cost is `1.25x` CPU time, `1.33x` bytes, and `13` more
allocations for this small state. That cost is opt-in and is the price of
persisting the full coupled state rather than only a broker offset.
