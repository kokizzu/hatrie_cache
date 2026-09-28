# Compact Timestamp Codec

`hatCodec.EncodeCompactTimestamps` adds a ClickHouse-style double-delta block
format for `int64` timestamp-like values. It selects the compressed form only
when its measured payload is smaller than fixed-width raw values. A conservative
32-sample gate avoids a full delta analysis for clearly high-entropy input.

## Format

The frame starts with `HTD1` and an encoding byte:

- `0`: count followed by little-endian fixed-width `int64` values.
- `1`: count, zigzag-encoded first value, zigzag-encoded first delta, then
  zigzag-encoded delta-of-delta values.

Difference arithmetic is modular in `uint64`, so arbitrary signed `int64`
sequences round-trip, including `MinInt64`/`MaxInt64` transitions. Decoding
rejects invalid headers, non-canonical uvarints, truncated payloads, trailing
bytes, and counts above the bounded block limit.

```go
encoded, err := hatCodec.EncodeCompactTimestamps(values)
decoded, err := hatCodec.DecodeCompactTimestamps(encoded)
```

## Benchmark

Five runs on an AMD Ryzen 9 5950X using 4,096 timestamps at a constant
1,000,000-unit interval:

| Operation | Raw control | Double-delta codec | Relative result |
| --- | ---: | ---: | --- |
| Encode | 8,306 ns/op, 40,960 B/op, 1 alloc | 14,989 ns/op, 4,864 B/op, 1 alloc | 1.80x CPU cost, 8.42x lower allocation |
| Decode | 7,692 ns/op, 32,768 B/op, 1 alloc | 7,292 ns/op, 32,768 B/op, 1 alloc | 1.05x faster, same result allocation |

The frame is 4,113 bytes versus 32,775 bytes for the framed raw control, or
7.97x smaller. For a high-entropy irregular block, the sample gate keeps the
raw representation: encode measured 8,270 ns/op versus 7,414 ns/op raw, with
the same 40,960 B/op allocation.

The encode CPU cost on regular blocks is the explicit tradeoff for storage and
wire reduction; the selector avoids imposing the full analysis cost on
high-entropy input.

Reproduce with:

```text
make test-codec-timestamps
make benchmark-codec-timestamps
make test-codec-timestamps-package
make race-codec-timestamps
```
