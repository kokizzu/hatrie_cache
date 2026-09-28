# Run-Length `uint64` Codec

This is a ClickHouse-inspired constant/run-length block codec for repeated
integer values. It is standalone in `hat/hatCodec` so callers can opt into it
for storage or wire blocks without changing existing formats.

## API

```go
frame := hatCodec.EncodeRunLengthUint64(values)
values, err := hatCodec.DecodeRunLengthUint64(frame, reuse)
```

`DecodeRunLengthUint64` reuses `reuse` when its capacity is sufficient. The
encoder emits an `HCR1` frame. It uses fixed-width little-endian raw values by
default and switches to RLE only when the measured RLE frame is smaller. A
64-value admission probe avoids scanning obviously high-entropy blocks; a late
run may therefore remain raw, which is intentional and preserves a cheap
fallback path.

## Frame

- Header: `HCR1`, version `1`, mode, canonical count uvarint.
- Raw mode: exactly `count * 8` little-endian `uint64` values.
- RLE mode: canonical token uvarints followed by either a value uvarint for a
  run or fixed-width literal values. The low token bit selects run (`1`) or
  literal (`0`); the remaining bits are the item count.
- Decoding rejects bad magic/version/mode, non-canonical or overflowing
  uvarints, zero-length tokens, count mismatches, truncation, trailing bytes,
  and oversized declarations before allocating a destination block.

## Benchmark

Command:

```text
make benchmark-codec-rle
```

The benchmark uses 4096 values on an AMD Ryzen 9 5950X, Go's `-benchmem`,
`-benchtime=200ms`, and five samples. Reported timings are the median sample.
The baseline is the existing fixed-width raw `HCR1` representation.

| Workload | Operation | Raw baseline | RLE codec | Result |
| --- | --- | ---: | ---: | ---: |
| 128 runs of 32 values | Encode | 7,565 ns, 40,960 B | 5,153 ns, 288 B | 1.47x faster, 142.2x fewer allocated bytes |
| 128 runs of 32 values | Decode | 4,186 ns, 1 B | 2,272 ns, 0 B | 1.84x faster, allocation-free |
| Unique high-entropy values | Encode | 7,481 ns, 40,960 B | 7,286 ns, 40,960 B | 1.03x faster, same allocation size |
| Unique high-entropy values | Decode | 4,275 ns, 1 B | 3,723 ns, 1 B | 1.15x faster, same allocation size |

Exact frame sizes are covered by `TestRunLengthUint64KnownFrameSizes`:

- Repeated input: 264 bytes versus 32,776 bytes raw, `124.15x` smaller.
- Unique input: 32,776 bytes in both modes.

The codec is worth keeping because the repeated-data win is large and the
admission gate prevents a high-entropy block from becoming larger. It is not a
replacement for the existing formats; callers should use the codec only where
the block's values have a meaningful chance of repetition.
