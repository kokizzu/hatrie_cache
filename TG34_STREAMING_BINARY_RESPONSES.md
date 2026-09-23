# T-G34 Streaming Binary Responses

T-G34 is implemented as an opt-in framed stream in `hat/hatHttp`. Existing
JSON, NDJSON, and journal-specific binary endpoints keep their current wire
formats and defaults.

## API

Use `StreamBinaryHTTPResponse` for an HTTP handler:

```go
err := hatHttp.StreamBinaryHTTPResponse(w, r, hatHttp.BinaryStreamOptions{}, func(
	ctx context.Context,
	stream *hatHttp.BinaryStreamWriter,
) error {
	for _, payload := range payloads {
		if err := stream.WriteChunk(ctx, payload); err != nil {
			return err
		}
	}
	return nil
})
if err != nil {
	// Record the error. The helper has already emitted an error frame when
	// possible, so the HTTP status cannot be changed after streaming starts.
	log.Printf("binary stream failed: %v", err)
}
```

`StreamBinary` provides the same producer lifecycle for any `io.Writer`.
`NewBinaryStreamReader` validates the response on the receiving side.

Defaults are a 64 KiB data-frame limit and a 4 KiB error-frame limit. The
maximum accepted values are 16 MiB and 64 KiB respectively. These limits apply
to decoded frame payloads, so a reader never allocates from an untrusted length
without checking it first.

## Wire format

Every frame has a fixed 24-byte little-endian header followed by its payload:

| Bytes | Field |
| --- | --- |
| 0..3 | `HTBS` magic |
| 4 | protocol version (`1`) |
| 5 | kind: data (`1`), end (`2`), or error (`3`) |
| 6..7 | reserved, must be zero |
| 8..15 | monotonically increasing sequence number |
| 16..19 | payload length (`uint32`) |
| 20..23 | CRC32C of the payload |

Data starts at sequence zero. The end or error frame consumes the next
sequence number and is terminal. A reader rejects bad magic/version, invalid
kind, oversized payloads, checksum failures, sequence gaps, and streams that
end after data without a terminal frame.

HTTP responses use content type
`application/vnd.hatrie.binary-stream`, `Cache-Control: no-store`, and flush
after each complete frame when the response writer implements `http.Flusher`.
The underlying writer supplies backpressure. Context cancellation is checked
before every read/write step; an arbitrary blocking `io.Writer` cannot be
interrupted by the library until that writer returns.

Producer errors are returned to the caller and, when the context and writer
are still usable, are also sent as a bounded error frame. Context cancellation
and an underlying write failure do not attempt a second write after the
connection is already closing.

## Verification

```text
make test-tg34-binary-stream
make test-tg34-binary-stream-package
make race-tg34-binary-stream
make vet-tg34-binary-stream
```

## Benchmark

The benchmark uses a counting `io.Writer`, 1 KiB payloads, five samples, and
`-benchtime=100ms` on an AMD Ryzen 9 5950X. The raw baseline writes the same
payloads without framing. Values are medians.

| Payload shape | Raw ns/op | Framed ns/op before header reuse | Framed ns/op after header reuse | Framed B/op after | Framed allocs/op after | Framed wire bytes/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 x 1 KiB | 0.7522 | 165.6 | 129.5 | 80 | 1 | 1,072 |
| 16 x 1 KiB | 4.645 | 1,456 | 1,050 | 80 | 1 | 16,792 |
| 64 x 1 KiB | 17.44 | 5,780 | 4,047 | 80 | 1 | 67,096 |

Reusing the writer-owned header improved framed CPU by `1.28x`, `1.39x`, and
`1.43x`, and reduced allocations from `3/18/66` to one per stream. Retained
writer bytes fell from `112/472/1,624` to `80` bytes per operation. The frame
protocol adds 48 bytes for a one-data-frame stream and 24 bytes per additional
frame, which is 4.69%, 2.49%, and 2.38% wire overhead for the three shapes.

The raw comparison is intentionally not presented as a speed win: framing
adds CRC, sequence, validation, and terminal-state guarantees. The feature is
therefore opt-in and is intended for bounded long-lived responses where
materializing the complete payload or using an unvalidated byte stream is the
larger operational risk.
