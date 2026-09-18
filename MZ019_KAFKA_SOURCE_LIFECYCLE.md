# MZ-019: Durable Kafka Source Lifecycle State

Kafka table sources now have an explicit operator lifecycle state. A source is
`running` by default; `Pause` stops ingestion and `Resume` enables it again.
The state is part of `KafkaTableSourceSnapshot`, so MZ-16 checkpoint stores can
persist the operator decision together with rows, offsets, and replay markers.

## In-Memory Control

```go
paused, err := source.Pause("planned maintenance")
if err != nil {
    return err
}

// ApplyBatch and ConsumeOnce return ErrKafkaTableSourcePaused.
running, err := source.Resume()
```

Each state transition increments `Generation`. Repeated `Pause` calls are
idempotent and preserve the first pause reason and generation. Reasons are
trimmed, reject control/format characters, and are bounded to 256 bytes.

## Durable Control

Use the same `KafkaTableSourceCheckpointStore` used for durable source batches:

```go
paused, err := source.PauseWithCheckpoint(ctx, store, "operator request")
if err != nil {
    return err
}

_, err = source.ResumeWithCheckpoint(ctx, store)
```

The lifecycle mutation and the complete source snapshot are committed under
the source lock. A failed checkpoint commit restores the previous lifecycle
state. Restoring an older snapshot with an empty lifecycle field is supported
and defaults it to `running`.

## Behavior And Cost

Paused sources reject direct `ApplyBatch`, `ConsumeOnce`, and
`ConsumeOnceWithCheckpoint` calls before accepting new records. The existing
running path does not perform lifecycle allocations. Five runs of
`make benchmark-mz019-kafka-source-lifecycle` on an AMD Ryzen 9 5950X measured
the in-memory pause/resume pair as:

| Operation | Raw ns/op samples | B/op | allocs/op |
| --- | --- | --- | --- |
| Pause + Resume | 84.92, 79.11, 84.86, 85.90, 89.49 | 0 | 0 |

Median is `84.92 ns/op` with zero allocations. Durable pause/resume uses the
checkpoint path and therefore pays the complete snapshot-copy cost documented
in [MZ016_KAFKA_SOURCE_CHECKPOINT.md](MZ016_KAFKA_SOURCE_CHECKPOINT.md).
