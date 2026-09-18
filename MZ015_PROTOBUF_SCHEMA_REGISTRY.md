# MZ-015 Protobuf Schema Registry Integration

`hat/hatSql` now supports Confluent Schema Registry Protobuf framing without
adding a Kafka client or a generated-schema dependency. The application
provides a fetcher for serialized `FileDescriptorSet` bytes and injects the
payload-to-row decoder. The shared layer handles bounded caching, descriptor
resolution, nested message indexes, and compatibility admission checks.

## Wire Format

The default payload format is:

```text
0x00 | schema ID (4-byte big endian) | message indexes | protobuf datum
```

Current Confluent message indexes use zigzag-varint encoding. The common path
`[0]` is encoded as one zero byte. The library also exposes
`ProtobufIndexEncodingUnsigned` for the deprecated serializer format. This
matches [Confluent's documented Protobuf wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format).

## Registry And Dynamic Messages

```go
registry, err := hatSql.NewProtobufSchemaRegistry(
    hatSql.ProtobufSchemaRegistryOptions{
        Fetch: func(schemaID uint32) ([]byte, error) {
            return schemaRegistry.GetFileDescriptorSet(schemaID)
        },
    },
)
if err != nil {
    return err
}

messageType, err := registry.ResolveMessage(7, []int{1, 0})
if err != nil {
    return err
}
message := dynamicpb.NewMessage(messageType.Descriptor())
```

The first descriptor-set file is treated as the root schema file. Index `0`
selects its first top-level message; later indexes select nested messages.
Descriptor sets and message paths are cached, so repeated records with the
same schema ID and message type avoid reparsing and dynamic type construction.

Defaults are 256 schemas, 1 MiB per schema, and 64 indexes per message path.
Hard limits are 100,000 schemas, 16 MiB per schema, and 128 indexes. These
limits prevent untrusted schema IDs or malformed paths from causing unbounded
retention.

## Kafka Table Decoder

Inject the actual protobuf-to-row conversion:

```go
decoder, err := hatSql.NewProtobufKafkaTableDecoder(
    registry,
    func(schemaID uint32, indexes []int, messageType protoreflect.MessageType, payload []byte, message hatSql.KafkaTableMessage) (hatSql.KafkaTableChange, error) {
        value := dynamicpb.NewMessage(messageType.Descriptor())
        if err := proto.Unmarshal(payload, value); err != nil {
            return hatSql.KafkaTableChange{}, err
        }
        return hatSql.KafkaTableChange{
            Key:       message.Key,
            Operation: hatSql.KafkaTableUpsert,
            Row:       rowFromDynamicMessage(value),
        }, nil
    },
)
```

Pass `decoder` to `KafkaTableSourceOptions.Decoder`. A Kafka tombstone skips
schema lookup and becomes a primary-key delete. Use
`NewProtobufKafkaTableDecoderWithEncoding` only when consuming a producer that
still emits the deprecated unsigned index encoding.

## Compatibility

`ValidateProtobufSchemaCompatibility` compares descriptor sets by fully
qualified message name and field number. It accepts optional field additions,
field removals, and the documented wire-compatible primitive kind changes. It
rejects required-field additions, unsafe field-number/type reuse, incompatible
message types, removed messages, and malformed descriptor sets. It supports
backward, forward, and full modes.

```go
if err := hatSql.ValidateProtobufSchemaCompatibility(
    previousDescriptorSet,
    candidateDescriptorSet,
    hatSql.ProtobufCompatibilityFull,
); err != nil {
    return fmt.Errorf("reject schema: %w", err)
}
```

The checker is intentionally conservative for complex oneof, enum-value, and
packed/repeated evolution. Registry-side compatibility remains authoritative;
the local check is an admission guard that fails closed rather than guessing.

## Benchmark

The benchmark processes 10,000 records using one schema ID and one message
path. The baseline fetches and reparses the descriptor set for each record.
The cached path warms the descriptor set and message type once, then performs
cached path resolution. Actual protobuf datum decoding and network latency are
outside this cache-boundary comparison.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Fetch and parse every record | 77,755,539 | 59,303,443 | 660,683 | 1.00x |
| Cached descriptor and message path | 827,796 | 3 | 0 | 93.9x faster |

See the raw samples in `BENCHMARK.md` and rerun
`make benchmark-mz015-protobuf-schema-registry` on target hardware.
