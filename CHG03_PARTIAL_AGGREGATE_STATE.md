# CH-G03 Partial Aggregate State

This is a ClickHouse-inspired public primitive for transferring mergeable
partial `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` state between workers. It avoids
shipping every input row and is independent of the default SQL execution path.

## API

```go
state := hatSql.SQLPartialAggregateState{
    Groups: []hatSql.SQLPartialAggregateGroup{{
        Key: "north", Value: "North", Count: 2,
        Sum: 7.5, HasSum: true,
        Min: 2, HasMin: true,
        Max: 5, HasMax: true,
    }},
}

wire, err := hatSql.EncodeSQLPartialAggregateState(state)
decoded, err := hatSql.DecodeSQLPartialAggregateState(wire)
err = hatSql.MergeSQLPartialAggregateState(&decoded, anotherState)
```

`Key` is the canonical grouping key produced by the query collation. `Value`
is the representative value used for output. The lowest `Ordinal` wins, which
keeps the first-input ordering rule when two workers contain the same group.
`MarshalBinary` and `UnmarshalBinary` are also provided for standard Go binary
codec integrations. On decode, signed integer values are normalized to
`int64`, unsigned values to `uint64`, and floating-point values to `float64`.

## Wire Format

The envelope has a fixed `HAGS` marker, a format version, a varint group count,
and compact per-field type tags. Signed integers use zig-zag varints; floating
point values use little-endian IEEE-754 bits. Supported scalar values are nil,
strings, byte slices, booleans, signed and unsigned integers, `float32`,
`float64`, and `time.Time`.

The decoder rejects an invalid marker or version, malformed varints, unknown
tags and flags, truncated fields, trailing bytes, more than 1,048,576 groups,
and scalar fields larger than 64 MiB. Count addition is checked for signed
integer overflow. These checks are required before using the codec with peer or
client-controlled data.

This is a transport/storage primitive, not an automatic distributed query
protocol. Callers still own schema compatibility, collation agreement,
authentication, retries, and exactly-once delivery. Unsupported application
types must be normalized before encoding.

## Benchmark

Linux amd64, AMD Ryzen 9 5950X, five samples per case, Go `-benchmem`, 1,024
groups with string keys and `COUNT`/`SUM`/`MIN`/`MAX` state. JSON is the baseline
using the same exported Go structs.

| Operation | Median ns/op | Payload bytes | B/op | allocs/op | Binary advantage |
|---|---:|---:|---:|---:|---:|
| Binary encode | 60,108 | 54,940 | 155,648 | 3 | 8.2x faster |
| JSON encode | 495,524 | 144,196 | 153,163 | 2 | baseline |
| Binary decode | 125,773 | 54,940 | 180,056 | 5,120 | 16.7x faster |
| JSON decode | 2,096,960 | 144,196 | 474,969 | 7,177 | baseline |

Binary is `2.62x` smaller on the wire, uses `2.64x` less memory while decoding,
and performs `1.40x` fewer decode allocations. Encoding allocates about `2%`
more bytes and one additional allocation because it builds the compact payload;
that is the measured tradeoff for substantially lower bandwidth and faster
decode.

Run the benchmark with:

```text
make benchmark-chg03-partial-aggregate
```

Correctness and malformed-input coverage:

```text
make test-chg03-partial-aggregate
```
