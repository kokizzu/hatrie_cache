# Default-Value Suppression Codec

`hatCodec.EncodeCompactUint64` now has a ClickHouse-style sparse-column
representation for unsigned integer blocks with many zero values. The encoder
selects raw, existing bit-packed, or zero-suppressed storage based on the
smallest exact representation. Zero suppression is admitted only when it is
strictly smaller than both alternatives and at least eight default values are
present.

## Format

The stream uses the `HCS1` header and an encoding mode. Mode `2` contains:

1. A uvarint row count.
2. A bitmap with one bit per row. A set bit means the row has a non-zero value.
3. Uvarint non-zero values in row order.

The decoder rejects truncated payloads, trailing bytes, non-canonical uvarints,
zero values marked as non-default, and non-zero unused bitmap padding bits. The
row count is bounded by the existing adaptive numeric limit before output
allocation.

Modes `0` and `1` contain raw and existing bit-packed payloads respectively.
The bit-packed payload is written directly into the final frame, avoiding an
intermediate buffer. Zero remains the suppressed default.

## Compatibility

Callers continue to use the existing APIs:

```go
encoded, err := hatCodec.EncodeCompactUint64(values)
decoded, err := hatCodec.DecodeCompactUint64(encoded)
```

`EncodeBitPackedUint64` remains available when a caller needs the old payload
format explicitly. Raw and bit-packed payloads are unchanged, and the compact
frame is a separate opt-in storage/wire format.

## Benchmark

Five runs on an AMD Ryzen 9 5950X, using 4,096 values with one non-zero
high-range value every 32 rows:

| Operation | Existing bit-packed control | Compact sparse mode | Result |
| --- | ---: | ---: | --- |
| Encode | 79,491 ns/op, 21,760 B/op, 1 alloc | 11,275 ns/op, 1,408 B/op, 1 alloc | 7.05x faster, 15.45x lower allocation |
| Decode | 169,243 ns/op, 32,768 B/op, 1 alloc | 8,834 ns/op, 32,768 B/op, 1 alloc | 19.16x faster, same output allocation |

The encoded payload is 1,287 bytes instead of 20,995 bytes, a 16.31x wire-size
reduction. Decode allocation is unchanged because both paths materialize the
same 4,096-value result. For dense 4,096-value blocks, compact encoding uses
the existing bit-packed representation directly: 9,542 ns/op and 4,864 B/op
versus 31,710 ns/op and 4,864 B/op for the control. The compact frame adds
five wire bytes to that dense payload.

Reproduce with:

```text
make test-codec-default-value-suppression
make benchmark-codec-default-value-suppression
make race-codec-default-value-suppression
```
