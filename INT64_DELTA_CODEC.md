# Signed `int64` Delta Codec

This is a ClickHouse-style delta block codec for monotonic IDs, counters, and
other signed integer series. The first value is fixed-width; subsequent values
use the cheapest applicable delta representation:

- signed-byte deltas when every modular delta fits `[-128, 127]`;
- canonical ZigZag varints for wider but still compressible deltas;
- fixed-width raw values when sampling or the full measured frame does not win.

```go
frame := hatCodec.EncodeInt64Delta(values)
values, err := hatCodec.DecodeInt64Delta(frame, reuse)
```

Modular two's-complement reconstruction preserves every `int64` bit pattern,
including transitions between `math.MaxInt64` and `math.MinInt64`.

## Frame and validation

`HID1` contains a version, mode, and canonical count uvarint. Raw mode stores
8-byte little-endian values. Byte-delta mode stores the first value followed by
one signed byte per delta. Varint-delta mode stores ZigZag-encoded deltas.
Decoding rejects invalid modes, non-canonical/overflowing varints, truncation,
trailing bytes, count mismatches, and oversized declarations before allocating
for an untrusted compressed frame.

## Benchmark

Command:

```text
make benchmark-codec-int64-delta
```

The benchmark uses 4096 values on an AMD Ryzen 9 5950X, Go's `-benchmem`,
`-benchtime=200ms`, and five samples. Timings below are median samples. The
baseline is a fixed-width raw `int64` frame.

| Workload | Operation | Raw baseline | Delta codec | Result |
| --- | --- | ---: | ---: | --- |
| Monotonic, step 10 | Encode | 6,679 ns, 40,960 B/op | 4,648 ns, 4,864 B/op | 1.44x faster, 8.42x fewer allocated bytes |
| Monotonic, step 10 | Decode | 3,906 ns, 1 B/op | 2,894 ns, 0 B/op | 1.35x faster, allocation-free |
| Noisy small deltas | Encode | 6,471 ns, 40,960 B/op | 4,579 ns, 4,864 B/op | 1.41x faster, 8.42x fewer allocated bytes |
| Noisy small deltas | Decode | 3,934 ns, 1 B/op | 2,854 ns, 0 B/op | 1.38x faster, allocation-free |
| High entropy | Encode | 6,692 ns, 40,960 B/op | 6,151 ns, 40,960 B/op | Raw fallback; effectively neutral |
| High entropy | Decode | 3,876 ns, 1 B/op | 3,935 ns, 1 B/op | Raw fallback; effectively neutral |

`TestInt64DeltaKnownFrameSize` verifies the representative wire sizes:

- Raw: 32,776 bytes.
- Byte-delta: 4,111 bytes, `7.97x` smaller.

The compact path is a net win for the intended ordered/counter workloads. The
sampling gate avoids paying the full analysis cost for high-entropy blocks, and
the byte-delta mode avoids varint parsing overhead for the common case.
