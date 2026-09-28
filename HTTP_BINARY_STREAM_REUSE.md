# Reusable Binary-Stream Payload Buffers

`hatHttp.BinaryStreamReader.NextInto` decodes a frame into a caller-owned
buffer. The returned `BinaryStreamFrame.Payload` aliases that buffer until the
caller reuses it. The existing `Next` method remains allocation-compatible and
unchanged for callers that prefer independent payload ownership.

```go
payload := make([]byte, 64<<10)
for {
	frame, err := reader.NextInto(ctx, payload)
	if err != nil {
		return err
	}
	consume(frame.Payload)
	if frame.Kind == hatHttp.BinaryStreamFrameEnd {
		break
	}
}
```

If the next frame is larger than `cap(payload)`, `NextInto` returns
`ErrBinaryStreamBufferTooSmall` and terminates that reader after consuming the
frame header. The caller can choose a larger buffer and restart from a new
stream. All existing bounds, sequence, checksum, truncation, and context
checks remain active.

This is the same ownership pattern used by high-throughput native streaming
interfaces: ClickHouse-style block transfer, Materialize differential batches,
and Tarantool iproto clients can reuse a bounded receive area instead of
allocating one payload per frame.

## Measurement

Machine: AMD Ryzen 9 5950X, linux/amd64. The fixture contains 64 data frames of
256 bytes plus one terminal frame. Command:

```text
make benchmark-http-stream-reuse
```

| Reader path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Existing `Next` | 8,635 | 18,024 | 131 |
| `NextInto` with one reusable 256-byte buffer | 4,962 | 1,896 | 68 |

`NextInto` is `1.74x` faster, retains `9.50x` fewer bytes per operation, and
performs `1.93x` fewer allocations. A post-change control benchmark of the
existing path measured `8,621 ns/op`, confirming that the refactor did not
materially regress `Next`. The optimization is opt-in because buffer lifetime
ownership is an API responsibility.

Verification:

```text
make test-http-stream-reuse
make test-http-stream-package
make race-http-stream-reuse
```
