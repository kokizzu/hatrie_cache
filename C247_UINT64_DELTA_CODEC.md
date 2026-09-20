# C247: Adaptive Uint64 Delta Codec

This adds opt-in ClickHouse-style numeric compression primitives to
`hatDataStructure`. They are useful for sorted journal offsets, timestamps,
sequence numbers, and time-series float columns.

```go
values := []uint64{1000, 1001, 1002, 1100}
payload, mode, err := hatDataStructure.EncodeUint64(values)
if err != nil {
	return err
}
decoded, decodedMode, err := hatDataStructure.DecodeUint64(payload)
```

`EncodeUint64` writes an `HDU1` header, the value count, and then chooses the
smaller of:

- raw little-endian `uint64` values; or
- the first value followed by unsigned varint deltas.

Decreasing input cannot be delta encoded. The adaptive API falls back to raw
encoding instead of expanding the payload. `EncodeUint64Delta` is available
when a caller wants strict delta mode and an error for decreasing input.
Decoding caps the declared value count at 16,777,216 and rejects bad magic,
unknown encodings, truncated varints, overflowing deltas, and trailing bytes.

The codecs do not change existing cache persistence or replication formats.
Callers can use the returned encoding mode to persist a payload as part of a
versioned format. The float codec records its valid bit count, rejects extra
bytes and nonzero padding, and preserves exact IEEE-754 bits. Its adaptive
path samples up to 256 transitions first, so high-entropy input can retain raw
encoding without a full sizing pass; a later-compressible sequence may
conservatively remain raw if its prefix is not representative. ALP-style
floating-point codecs remain separate follow-up work.

## Measurement

Five 200 ms benchmark samples on an AMD Ryzen 9 5950X, Linux amd64, with one
million values:

| Workload | Median time | Heap | Result |
| --- | ---: | ---: | --- |
| Adaptive encode, monotone input | 2.252 ms | 1.057 MB | Delta mode, 7.95x smaller than raw |
| Raw fixed-width encode control | 2.234 ms | 8.397 MB | Baseline |
| Adaptive encode, random input | 2.267 ms | 8.397 MB | Raw fallback, 1.015x the control time, no size regression |
| Delta decode | 4.896 ms | 8.389 MB | 2.53x slower than raw decode |
| Raw decode | 1.932 ms | 8.389 MB | Baseline |

The tradeoff is intentional: monotone data gets nearly 8x lower wire/storage
payload for essentially the same encode CPU, while decoding pays about 2.5x
CPU. Random data keeps the raw size and adds only the adaptive inspection cost.
The API remains opt-in so workloads that prioritize decode CPU can retain raw
encoding.

## Gorilla Float64 Measurement

Five 200 ms samples on the same host, with one million step-valued `float64`
samples unless noted:

| Workload | Median time | Heap | Result |
| --- | ---: | ---: | --- |
| Adaptive Gorilla encode | 9.526 ms | 163,840 B | 51.2x smaller than raw; 5.28x slower encode |
| Raw fixed-width encode control | 1.805 ms | 8,396,800 B | Baseline |
| Adaptive encode, true random IEEE bits | 1.916 ms | 8,396,800 B | Raw fallback; 1.06x control time, no size regression |
| Gorilla decode | 6.362 ms | 8,388,608 B | 3.03x slower than raw decode |
| Raw decode | 2.102 ms | 8,388,608 B | Baseline |

The Gorilla tradeoff is worthwhile for bandwidth- or storage-bound, highly
repetitive time-series data, but not for decode-CPU-bound workloads. The
adaptive API therefore remains opt-in and never expands a high-entropy payload.

Run the focused verification and benchmark with:

```text
make test-c247-delta-codec
make test-c247-delta-codec-package
make race-c247-delta-codec
make vet-c247-delta-codec
make benchmark-c247-delta-codec
```
