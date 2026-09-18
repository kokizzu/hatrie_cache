# CH-046 Kafka Table Source

`hat/hatSql` now includes a dependency-free adapter contract for exposing a
Kafka topic as a primary-key SQL table. It does not open broker connections or
choose a Kafka client library. An application-specific client implements
`KafkaTableConsumer`, polls records, and hands batches to the source.

## Guarantees

- Upserts and tombstones are applied as one source batch.
- All records newer than the stored partition checkpoints are decoded before
  any row is changed. A decoder failure leaves rows and checkpoints unchanged.
- One high-watermark is stored per source partition. Offset `0` is valid.
- A successful batch advances checkpoints only after its row changes succeed.
- Reusing a committed transaction ID is a no-op. If no ID is supplied, the
  source derives one from the batch's partition high-watermarks.
- `ConsumeOnce` commits to the broker only after local application succeeds.
  A broker commit failure is safe to retry because local replay is idempotent.
- `Snapshot` and `Restore` include rows, offsets, and committed transaction
  envelopes so a backup can resume without replaying committed transactions.

The checkpoint values passed to `KafkaTableConsumer.Commit` are the last
successfully applied offsets. Kafka clients that commit next offsets must add
one at their client boundary.

## Setup

```go
source, err := hatSql.NewKafkaTableSource(hatSql.KafkaTableSourceOptions{
    Source:  "orders-kafka",
    Table:   "orders",
    Topic:   "orders",
    Decoder: hatSql.KafkaTableJSONDecoder,
})
if err != nil {
    return err
}

result, err := source.ConsumeOnce(ctx, consumer)
if err != nil {
    return err
}
_ = result.ConsumerCommitted
```

`KafkaTableJSONDecoder` treats a JSON object as an upsert keyed by
`KafkaTableMessage.Key`. A Kafka tombstone (`Value == nil`) deletes that key.
For typed or binary payloads, provide a custom decoder:

```go
Decoder: func(message hatSql.KafkaTableMessage) (hatSql.KafkaTableChange, error) {
    row, err := decodeOrder(message.Value)
    if err != nil {
        return hatSql.KafkaTableChange{}, err
    }
    return hatSql.KafkaTableChange{
        Key:       message.Key,
        Operation: hatSql.KafkaTableUpsert,
        Row:       row,
    }, nil
},
```

The source can also be driven directly when the application already owns the
poll loop:

```go
result, err := source.ApplyBatch(hatSql.KafkaTableBatch{
    TransactionID: "orders-tx-42",
    Messages: []hatSql.KafkaTableMessage{
        {
            Topic: "orders", Partition: "0", Offset: 42,
            Key: "order-7", Value: []byte(`{"status":"paid"}`),
        },
    },
})
```

Use `source` as the `Source` in the normal SQL catalog resolver. Reads return a
deterministic key-sorted row snapshot and can use all existing SQL filtering,
grouping, ordering, and projection behavior.

## Backup And Restore

Persist `source.Snapshot()` with the application's existing backup format and
restore it into a source created with the same `Source`, `Table`, `Topic`, and
`SourceKind` values:

```go
snapshot := source.Snapshot()
// persist snapshot with the normal application backup.

restored, err := hatSql.NewKafkaTableSource(options)
if err != nil {
    return err
}
if err := restored.Restore(snapshot); err != nil {
    return err
}
```

The adapter does not silently persist snapshots to disk. The caller remains
responsible for durable storage, encryption, retention, and atomic replacement
of its backup file. Snapshot rows can contain application data and must be
treated as sensitive backup material.

## Limits And Tradeoff

The default batch limit is 10,000 messages; the absolute configured maximum is
1,000,000. One record value is limited to 16 MiB. Source labels are limited to
256 bytes, primary keys to 4 KiB, and explicit transaction IDs to 512 bytes.

The benchmark compares a 10,000-record per-message checkpoint loop with the
batched source path using the same row ownership copy:

| Path | Median time | Median memory | Median allocations | Relative CPU | Relative memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Per-message baseline | 4.43 ms | 3.81 MB | 20,253 | 1.00x | 1.00x |
| Batched durable source | 4.52 ms | 4.15 MB | 20,283 | 1.02x | 1.09x |

The measured overhead is small but real. It buys atomic decode/apply, replay
markers, consumer commit ordering, and restorable source state; this adapter
should not be selected solely as a faster ingestion loop. Re-run
`make benchmark-ch046-kafka-table-source` on the target deployment hardware
before changing batch limits or decoder ownership rules.

## Verification

```text
make test-ch046-kafka-table-source
make benchmark-ch046-kafka-table-source
make race-ch046-kafka-table-source
make vet-ch046-kafka-table-source
make test-ch046-kafka-table-source-package
```

The tests cover offset zero, replay after restore, failed-batch atomicity,
upserts, tombstones, topic and batch validation, and commit-after-apply
ordering.
