# C247: Adaptive Uint64 Delta Codec

This adds an opt-in ClickHouse-style numeric compression primitive to
`hatDataStructure`. It is useful for sorted journal offsets, timestamps,
sequence numbers, and other nondecreasing `uint64` columns.

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

The codec does not change existing cache persistence or replication formats.
Callers can use the returned encoding mode to persist the payload as part of a
versioned format. Gorilla and ALP-style floating-point codecs remain separate
follow-up work; this feature covers the safe integer-delta portion of C247.

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

Run the focused verification and benchmark with:

```text
make test-c247-delta-codec
make test-c247-delta-codec-package
make race-c247-delta-codec
make vet-c247-delta-codec
make benchmark-c247-delta-codec
```
