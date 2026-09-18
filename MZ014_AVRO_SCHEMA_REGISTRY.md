# MZ-014 Avro Schema Registry Integration

`hat/hatSql` now provides the schema-registry boundary needed by a Kafka/CDC
source that uses Confluent Avro framing. The feature deliberately does not
ship an Avro datum codec or an HTTP client. Applications inject those two
pieces while the shared code handles framing, bounded caching, concurrent
fetch coalescing, and compatibility validation.

## Registry Cache

```go
registry, err := hatSql.NewAvroSchemaRegistry(hatSql.AvroSchemaRegistryOptions{
    Fetch: func(schemaID uint32) ([]byte, error) {
        return registryClient.GetSchema(schemaID)
    },
    MaxSchemas:     256,
    MaxSchemaBytes: 1 << 20,
})
```

The default cache holds 256 schemas and accepts schemas up to 1 MiB. The hard
limits are 100,000 entries and 16 MiB per schema. Cache misses for the same ID
are single-flight: concurrent callers wait for one fetch. Eviction is bounded
least-recently-used-by-clock, so a registry outage cannot grow the cache
without limit.

`Borrow` returns immutable cached bytes without an allocation and `Schema`
returns an owned copy. Borrowed bytes must not be modified. The cache copies
fetcher output before retaining it, so a reused HTTP response buffer cannot
corrupt a cached schema.

## Confluent Kafka Payloads

Confluent Avro payloads are parsed as:

```text
0x00 | schema ID (4-byte big endian) | Avro datum bytes
```

Create a Kafka table decoder by injecting the actual Avro library:

```go
decoder, err := hatSql.NewAvroKafkaTableDecoder(
    registry,
    func(schemaID uint32, schema, datum []byte, message hatSql.KafkaTableMessage) (hatSql.KafkaTableChange, error) {
        row, err := decodeAvroWithYourLibrary(schema, datum)
        if err != nil {
            return hatSql.KafkaTableChange{}, err
        }
        return hatSql.KafkaTableChange{
            Key:       message.Key,
            Operation: hatSql.KafkaTableUpsert,
            Row:       row,
        }, nil
    },
)
```

Pass `decoder` to `KafkaTableSourceOptions.Decoder`. Tombstones bypass schema
resolution and become primary-key deletes. A malformed magic byte, short
header, missing schema, oversized schema, or injected decode error is returned
without applying the source row.

## Compatibility

`ValidateAvroSchemaCompatibility` checks primitive-field record evolution in
backward, forward, or full mode. It supports primitive unions and Avro numeric
promotion (`int` to `long`/`float`/`double`, `long` to `float`/`double`, and
`float` to `double`). New reader fields require defaults; incompatible field
types and record-name changes are rejected. Complex nested named-type changes
fail closed as unsupported rather than being guessed.

```go
err := hatSql.ValidateAvroSchemaCompatibility(
    previousSchema,
    candidateSchema,
    hatSql.AvroCompatibilityFull,
)
```

Run this check before publishing a new registry version when the application
needs a local admission guard. Registry-side compatibility remains the source
of truth for deployments using a remote schema registry.

## Security And Operations

- Keep schema-registry credentials and endpoint configuration in the injected
  fetcher; they are not retained in the cache or snapshots.
- Keep `MaxSchemas` and `MaxSchemaBytes` bounded for untrusted schema IDs.
- Treat schema bytes as untrusted input and let the injected decoder enforce
  datum limits before constructing a `Row`.
- Do not mutate `Borrow` results. Use `Schema` when handing bytes to code that
  cannot honor the read-only contract.
- A cache miss/fetch failure is visible to the Kafka table source, so the
  source batch does not advance its offsets or consumer commit.

## Benchmark

The benchmark processes 10,000 records carrying one schema ID. The baseline
fetches and allocates the schema for every record. The cached path warms the
same ID once and then uses `Borrow`.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Fetch every record | 480,615 | 800,137 | 10,004 | 1.00x |
| Bounded hot cache | 163,314 | 0 | 0 | 2.94x faster |

The cache benchmark excludes the actual Avro datum decode and network latency;
it measures the shared registry boundary only. See the raw samples in
`BENCHMARK.md` and rerun `make benchmark-mz014-avro-schema-registry` on target
hardware.
