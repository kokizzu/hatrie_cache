# Independent Compressed Blocks

`hat/hatCodec` provides an opt-in framed stream for payloads that benefit from
independent compressed blocks:

```go
encoded, err := hatCodec.EncodeCompressedBlocks(payload, hatCodec.CompressedBlockOptions{
	BlockSize: 64 << 10,
	Level:     flate.BestSpeed,
})
if err != nil {
	return err
}

decoded, err := hatCodec.DecodeCompressedBlocks(encoded)
if err != nil {
	return err
}
```

The zero-value options use 64 KiB blocks and `flate.BestSpeed`. `BlockSize` must
be between 1 byte and 64 MiB. `Level` accepts the raw DEFLATE levels from
`flate.HuffmanOnly` through `flate.BestCompression`; zero selects the default
`BestSpeed` setting.

## Frame Layout

The stream starts with the four-byte ASCII magic `HCB1`. Each following block
contains:

1. A uvarint raw length.
2. A uvarint payload length. Its high bit means that the payload is stored raw
   because compression was not smaller.
3. A little-endian CRC32-IEEE checksum of the raw block.
4. Either the raw bytes or a raw DEFLATE stream.

There are no column names or schema assumptions in this format. The caller can
place any already-encoded payload inside it, including RowBinary or a journal
envelope. A receiver can validate each block independently and does not need to
hold a complete DEFLATE stream in memory to decode it.

The decoder rejects invalid magic, malformed lengths, truncated blocks,
oversized blocks, decompression overruns, and checksum mismatches. It caps one
decoded block at 64 MiB and the complete decoded stream at 1 GiB.

## When To Use It

Use this format when block-local integrity, bounded recovery, or independently
retryable transfer units matter. A single gzip stream remains preferable for
the smallest payload or the fastest decode when the whole payload is handled
as one unit. The format is not automatically negotiated and does not change
the existing JSON, protobuf, or gzip defaults.

## Measured Tradeoff

On an AMD Ryzen 9 5950X, five-run medians over the repository's repetitive
1.3 MiB payload were:

| Operation | Independent blocks | Single gzip | Relative result |
| --- | ---: | ---: | --- |
| Encode | 324,365 ns/op | 543,444 ns/op | 1.68x faster |
| Decode | 294,150 ns/op | 201,512 ns/op | 1.46x slower |
| Encoded wire | 1,710 B | 1,492 B | 1.15x larger |
| Encode heap | 1,429,946 B / 23 allocs | 1,211,152 B / 25 allocs | 1.18x higher heap; 1.09x fewer allocs |
| Decode heap | 817,959 B / 31 allocs | 507,274 B / 25 allocs | 1.61x higher heap; 1.24x more allocs |

Run the focused tests and benchmark with:

```sh
make test-compressed-blocks
make benchmark-compressed-blocks
```
