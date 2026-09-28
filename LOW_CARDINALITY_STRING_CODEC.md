# Low-Cardinality String Codec

`hatCodec.EncodeLowCardinalityStrings` adds a ClickHouse-style dictionary
representation for repeated string columns. It automatically selects a
dictionary only when the complete dictionary payload is smaller than the raw
length-prefixed payload. High-cardinality input is admitted through a fixed
64-sample, allocation-free check and stays on the raw path without building a
full dictionary.

## Format

The frame starts with `HLD1` and an encoding byte:

- `0`: raw count followed by length-prefixed strings.
- `1`: row count, dictionary count, first-seen dictionary strings, then one
  uvarint dictionary index per row.

Dictionary order is deterministic. The decoder bounds row count, dictionary
count, individual string size, and total string bytes; it rejects duplicate
dictionary entries, invalid indexes, non-canonical uvarints, malformed lengths,
and trailing bytes.

```go
encoded, err := hatCodec.EncodeLowCardinalityStrings(values)
decoded, err := hatCodec.DecodeLowCardinalityStrings(encoded)
```

The selector keeps raw encoding for high-cardinality data. The raw format is
also retained inside the frame as the automatic fallback; no caller needs to
guess the cardinality first.

## Benchmark

Five runs on an AMD Ryzen 9 5950X using 4,096 values drawn from 16 repeated
22-byte strings:

| Operation | Raw control | Dictionary codec | Relative result |
| --- | ---: | ---: | --- |
| Encode | 41,102 ns/op, 139,264 B/op, 1 alloc | 72,198 ns/op, 39,512 B/op, 12 allocs | 1.76x CPU cost, 3.52x lower allocation |
| Decode | 99,149 ns/op, 163,840 B/op, 4,097 allocs | 29,278 ns/op, 67,112 B/op, 21 allocs | 3.39x faster, 2.44x lower bytes, 195x fewer allocs |

The raw payload is 94,210 bytes; the dictionary frame is 4,472 bytes, or
21.07x smaller. Encoding pays map/dictionary construction CPU and allocations,
which is appropriate for storage/wire savings and is measured explicitly.

For a 4,096-value unique-string workload, the sample admission path avoids the
full map: compact encoding measured 40,263 ns/op and 65,536 B/op versus
52,536 ns/op and 106,496 B/op for the raw control. The compact frame adds only
its five-byte header when it falls back to raw.

Reproduce with:

```text
make test-codec-low-cardinality
make benchmark-codec-low-cardinality
make test-codec-low-cardinality-package
make race-codec-low-cardinality
```
