# TT-045 Tuple-Level Compression

`hatDataStructure.TupleCompressor` is an opt-in codec for large opaque tuples
or values. It is inspired by Tarantool's tuple-oriented storage model, but it
does not change existing cache, snapshot, journal, storage, or peer-wire
defaults. Callers choose where compressed frames are stored or transferred.

## Behavior

The zero-value `TupleCompressionOptions` selects adaptive ZSTD compression for
new compressors. The adaptive policy:

- leaves values below 256 bytes uncompressed;
- samples up to 512 evenly distributed bytes and skips ZSTD for high-entropy
  input;
- keeps the raw value when compression does not save at least 16 bytes;
- rejects values larger than 64 MiB by default.

Both compressed and raw results use the self-describing `HTC1` frame. Its
20-byte header records the algorithm, format version, header size, original
length, payload length, and CRC32. A decoder therefore does not need an
out-of-band compression flag or tuple size. The CRC32 detects corruption and
truncation; it is not an authenticity or confidentiality mechanism.

The compressor retains its ZSTD encoder/decoder and a reusable compression
scratch buffer. After warm-up, a successful compressed call allocates only the
returned frame. A single compressor is safe for concurrent use, but calls are
serialized; use multiple compressors when independent parallel streams are
needed.

## Usage

```go
compressor, err := hatDataStructure.NewTupleCompressor(
    hatDataStructure.TupleCompressionOptions{},
)
if err != nil {
    return err
}
defer compressor.Close()

frame, err := compressor.Compress(tuple)
if err != nil {
    return err
}

info, err := hatDataStructure.InspectTupleCompressionFrame(frame)
if err != nil {
    return err
}
decoded, err := compressor.Decompress(frame)
if err != nil {
    return err
}
_ = info
_ = decoded
```

For an explicit raw framed value, set `Algorithm` to
`TupleCompressionNone`. For explicit ZSTD, set it to `TupleCompressionZSTD`.
`MinSize`, `MinSavingsBytes`, and `MaxTupleSize` can be overridden within the
validated bounds. The maximum is enforced before decompression allocates an
output buffer, which limits malformed or hostile frames' memory demand.

## Compatibility and Security

Frames reject an invalid magic value, unsupported version or algorithm,
malformed header lengths, mismatched payload lengths, CRC failures, and
oversized original or payload lengths. The decoder never treats an arbitrary
unframed byte slice as a valid tuple. This makes the codec suitable for a
trusted storage or transport boundary, but callers still need authentication,
authorization, encryption, and replay protection for an untrusted network.

Existing persistence and peer protocols remain unchanged because this is an
importable data-structure API, not an automatic format migration. Persist the
frame bytes exactly and pass them back to `Decompress`; changing a configured
maximum later can intentionally reject older oversized frames.

## Measured Tradeoff

The benchmark uses five samples per case on an AMD Ryzen 9 5950X. The input is
96 KiB. `make benchmark-tt045-c309` runs the complete package benchmark.

| Case | Median ns/op | Median B/op | Allocs/op | Input | Frame | Interpretation |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Adaptive repeated input, ZSTD | 21,259 | 276 | 1 | 98,304 B | 98 B | 1,003.1x smaller on the wire; 1.65x raw-copy CPU |
| Adaptive random input, raw fallback | 14,006 | 73,728 | 1 | 98,304 B | 98,324 B | 1.09x raw-copy CPU; 20 B framing cost |
| ZSTD decompression | 43,610 | 98,375 | 1 | 98 B | 98,304 B | Output allocation is returned to the caller |
| Raw-copy baseline | 12,854 | 98,304 | 1 | 98,304 B | n/a | Copy-only comparison |

Raw samples:

```text
zstd_compress: 22287, 21259, 20700, 19955, 21637 ns/op; 278, 271, 276, 261, 278 B/op; 1 alloc/op
zstd_decompress: 46278, 45436, 42843, 43610, 42804 ns/op; 98375, 98376, 98377, 98375, 98374 B/op; 1 alloc/op
raw_copy: 14976, 14430, 12049, 12854, 12188 ns/op; 98304 B/op; 1 alloc/op
random_fallback: 13952, 14595, 13916, 14006, 14229 ns/op; 73728 B/op; 1 alloc/op
wire_repeated: 22204, 22283, 24077, 22510, 22461 ns/op; 98 frame-bytes; 98304 input-bytes; 112 B/op; 1 alloc/op
wire_random: 21839, 20773, 21143, 21042, 20885 ns/op; 98324 frame-bytes; 98304 input-bytes; 106496 B/op; 1 alloc/op
```

The compressed case spends extra CPU to remove 99.90% of the payload bytes.
The random case avoids ZSTD work through entropy admission and pays only the
framing overhead. The codec retains its scratch capacity, while returned
frames and decompressed values are caller-owned allocations. Applications
should enable this for values where bandwidth or retained storage matters more
than compression CPU; small or latency-critical values can use
`TupleCompressionNone` or remain outside this codec.
