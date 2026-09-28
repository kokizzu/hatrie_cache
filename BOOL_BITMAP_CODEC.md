# Boolean Bitmap Codec

This is a ClickHouse-inspired bitmap representation for boolean and nullable
presence masks. It stores eight values per byte in an `HCB1` frame and is
available from `hat/hatCodec`:

```go
frame := hatCodec.EncodeBoolBitmap(values)
values, err := hatCodec.DecodeBoolBitmap(frame, reuse)
```

The decoder reuses `reuse` when its capacity is sufficient. The payload uses
least-significant-bit-first ordering within each byte. Unused bits in the final
byte must be zero.

## Validation

The decoder requires the exact `HCB1` header, version, canonical count
uvarint, exact `ceil(count/8)` payload length, and zero padding. It rejects
truncation, trailing bytes, bad padding, overflowing/non-canonical counts, and
large declarations before allocating a destination block.

## Benchmark

Command:

```text
make benchmark-codec-bitmap
```

The benchmark uses 4096 values (`true` every fifth position) on an AMD Ryzen
9 5950X, Go's `-benchmem`, `-benchtime=200ms`, and five samples. Timings below
are the median sample. The raw baseline stores one byte per boolean with the
same header and count framing.

| Operation | Raw byte baseline | Bitmap codec | Result |
| --- | ---: | ---: | --- |
| Encode | 2,077 ns, 4,864 B/op | 3,276 ns, 576 B/op | 1.58x slower CPU, 8.44x fewer allocated bytes |
| Decode with reused destination | 2,181 ns, 0 B/op | 551.6 ns, 0 B/op | 3.95x faster, allocation-free |

Exact frame sizes are covered by `TestBoolBitmapKnownFrameSize`:

- Raw baseline: 4,103 bytes.
- Bitmap: 519 bytes, `7.91x` smaller.

Encoding pays extra bit-packing CPU, but the resulting frame is substantially
smaller and decode is faster because the lookup table expands eight values at a
time. The compact format is the default API; callers that prioritize encode
CPU over bandwidth can retain their existing byte-per-boolean format.
