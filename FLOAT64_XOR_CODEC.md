# `float64` XOR Codec

This is a ClickHouse/Gorilla-inspired block codec for floating-point time
series. Consecutive values are XORed; unchanged values use a one-byte token,
and changed values store only the non-zero byte span. Blocks whose sampled
compression is not promising stay on the raw path, and the full candidate is
also discarded when it is not smaller.

```go
frame := hatCodec.EncodeFloat64XOR(values)
values, err := hatCodec.DecodeFloat64XOR(frame, reuse)
```

The codec preserves the exact `uint64` representation, including signed zero,
infinities, and NaN payloads. The decoder reuses `reuse` when possible.

## Frame and validation

`HGF1` frames contain a version, mode, canonical count uvarint, and either
fixed-width raw values or an XOR stream. XOR descriptors encode leading-byte
count and significant-byte count; invalid spans, truncation, trailing bytes,
non-canonical counts, and oversized declarations are rejected. A preflight
pass validates an XOR stream before allocating a destination for an untrusted
frame.

## Benchmark

Command:

```text
make benchmark-codec-float-xor
```

The benchmark uses 4096 values on an AMD Ryzen 9 5950X, Go's `-benchmem`,
`-benchtime=200ms`, and five samples. Timings below are median samples. The
baseline is a fixed-width raw `float64` frame.

| Workload | Operation | Raw baseline | XOR codec | Result |
| --- | --- | ---: | ---: | --- |
| Repeated value | Encode | 7,357 ns, 40,960 B/op | 9,520 ns, 4,864 B/op | 1.29x slower CPU, 8.42x fewer allocated bytes |
| Repeated value | Decode | 4,068 ns, 1 B/op | 2,860 ns, 0 B/op | 1.42x faster, allocation-free |
| Smooth values | Encode | 7,362 ns, 40,960 B/op | 7,748 ns, 40,960 B/op | 1.05x slower; raw fallback stays close to baseline |
| Smooth values | Decode | 4,082 ns, 1 B/op | 4,010 ns, 1 B/op | Effectively neutral |
| High-entropy values | Encode | 7,990 ns, 40,960 B/op | 7,762 ns, 40,960 B/op | Effectively neutral; raw fallback |
| High-entropy values | Decode | 4,488 ns, 1 B/op | 4,156 ns, 1 B/op | Effectively neutral; raw mode |

Exact repeated-block frame sizes are covered by
`TestFloat64XORKnownFrameSize`:

- Raw: 32,776 bytes.
- XOR: 4,111 bytes, `7.97x` smaller.

The encode CPU cost is intentional: this format is best for storage or wire
paths where repeated values and bandwidth matter. High-entropy and marginally
compressible blocks remain raw instead of paying a full candidate-build cost.
