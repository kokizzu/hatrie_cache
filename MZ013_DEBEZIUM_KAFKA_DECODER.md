# MZ-013: Debezium Kafka Envelope Normalization

`hat/hatSql` now has a dependency-free decoder for Debezium JSON change
events. It converts Debezium's envelope into the existing
`KafkaTableChange` contract, so a Kafka table source can consume create,
snapshot-read, update, delete, and tombstone records without connector-specific
row mutation code.

## Use

```go
decoder, err := hatSql.NewDebeziumKafkaTableDecoder(
    hatSql.DebeziumKafkaTableDecoderOptions{},
)
if err != nil {
    return err
}

source, err := hatSql.NewKafkaTableSource(hatSql.KafkaTableSourceOptions{
    Source:  "orders-cdc",
    Table:   "orders",
    Topic:   "db.public.orders",
    Decoder: decoder,
})
```

The convenience function `DebeziumKafkaJSONDecoder` uses the default limits.
The factory allows smaller limits when a source has a tighter input budget:

```go
decoder, err := hatSql.NewDebeziumKafkaTableDecoder(
    hatSql.DebeziumKafkaTableDecoderOptions{
        MaxPayloadBytes: 1 << 20,
        MaxKeyBytes:     1024,
    },
)
```

## Supported Events

Debezium's `op` values are normalized as follows:

| Debezium operation | Kafka table change | Row used |
| --- | --- | --- |
| `c` create | `KafkaTableUpsert` | `after` |
| `r` snapshot read | `KafkaTableUpsert` | `after` |
| `u` update | `KafkaTableUpsert` | `after` |
| `d` delete | `KafkaTableDelete` | Kafka record key |
| Kafka tombstone (`Value == nil`) | `KafkaTableDelete` | Kafka record key |

The decoder accepts both the compact event object and the Kafka Connect JSON
converter shape with a `payload` object. Unsupported operations such as
truncate (`t`) are rejected because the current Kafka table source has no
table-wide truncate operation.

The Kafka record key is required. Plain keys are trimmed; structured JSON keys
are decoded and re-encoded into a stable compact representation so equivalent
object key ordering does not create duplicate table rows. The default maximum
payload is 16 MiB and the default maximum key is 4 KiB. These limits match the
existing Kafka source safety bounds and can only be reduced by decoder options.

Debezium documents the standard envelope fields and operation codes in its
[PostgreSQL connector event documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).

## Validation

The decoder rejects malformed JSON, non-object `before`/`after` values,
missing keys, unsupported operations, missing `after` rows for create/read/
update events, and over-limit payloads. The source still clones accepted rows
before retaining them, so decoder output remains caller-owned until source
application.

## Benchmark

The benchmark compares a direct JSON parse of both `before` and `after` with
the full decoder path on the same update event. Five samples were run with
`make benchmark-mz013-debezium-kafka` on an AMD Ryzen 9 5950X:

| Path | Raw ns/op samples | B/op | allocs/op |
| --- | --- | --- | --- |
| Direct parse baseline | 2952, 2853, 2821, 2779, 2919 | 1112 | 26 |
| Debezium decoder | 2964, 3123, 2938, 2875, 3028 | 1144 | 26 |

Median overhead is approximately 3.9% CPU time and 2.9% bytes, with no
additional allocations. That cost buys operation/shape validation, structured
key canonicalization, tombstone handling, and the schema/payload envelope
support.
