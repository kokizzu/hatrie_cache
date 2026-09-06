# Codec Metrics

`hat/hatMetrics` provides allocation-free counters for measuring compression
tradeoffs without retaining payloads or per-operation samples.

```go
metrics := hatMetrics.NewCodecMetrics()
metrics.RecordCompression(uncompressedSize, compressedSize, compressionCPU)
metrics.RecordDecompression(compressedSize, uncompressedSize, decompressionCPU)

snapshot := metrics.Snapshot()
fmt.Println(snapshot.CompressionRatio())
```

Compression and decompression totals are separate because stored or wire data
can be written once and read many times. The snapshot includes input bytes,
output bytes, and CPU nanoseconds for both directions. `CompressionRatio()` is
`compressed_bytes / uncompressed_bytes`; a value below `1` means the encoded
representation is smaller. It returns `0` when no compression input has been
recorded.

The metrics object is safe for concurrent recording. Snapshot fields are
individual atomic reads and therefore may represent adjacent updates when
writers record concurrently. Nil receivers are no-ops, and non-positive CPU
durations are ignored. Recording does not change codec behavior, storage
format, or wire format.
