# Compact Peer Protocol

`hatPeer.CompactProtocol` is an importable, Tarantool iProto-inspired binary
envelope for peer adapters. It keeps the existing HTTP/2, gRPC, protobuf, and
JSON paths unchanged. `hatPeer.CompactMultiplexer` adds bounded correlation-ID
allocation and out-of-order response routing, but deliberately does not own a
socket or start a reader goroutine.

## Wire Format

Each frame is:

| Field | Encoding |
|---|---|
| Body length | canonical unsigned varint; excludes this prefix |
| Magic | two bytes: `HP` |
| Version | one byte, currently `1` |
| Kind | `1=request`, `2=response`, `3=error` |
| Flags | one byte; only the low two bits are currently reserved |
| Request ID | canonical unsigned varint; non-zero |
| Command length and command | canonical unsigned varint followed by bytes |
| Payload length and payload | canonical unsigned varint followed by bytes |

The decoder checks the length prefix before allocating the body, rejects
non-canonical varints, validates the version/kind/flags, and enforces command
and payload limits. Defaults are a 4 MiB body, 256-byte command, and a payload
limit just below the body limit. The maximum configurable body is 64 MiB.

## Adapter Flow

```go
protocol, err := hatPeer.NewCompactProtocol(hatPeer.CompactProtocolOptions{})
if err != nil {
	return err
}
multiplexer := hatPeer.NewCompactMultiplexer()

request, pending, err := multiplexer.Request([]byte("SETSTR"), payload)
if err != nil {
	return err
}
if err := protocol.Write(conn, request); err != nil {
	multiplexer.Cancel(pending.ID())
	return err
}

// One reader loop should call protocol.Read(conn) and then:
response, err := protocol.Read(conn)
if err != nil {
	return err
}
if err := multiplexer.Resolve(response); err != nil {
	return err
}
result, err := pending.Wait(ctx)
```

Writes must be serialized by the adapter when the underlying connection does
not support concurrent writes. Responses may arrive in any order. A pending
request is canceled when its wait context expires. The default multiplexer
retains at most 1,024 pending requests; use
`NewCompactMultiplexerWithOptions` to select a different bound up to 1,048,576.

The protocol does not provide authentication, encryption, compression,
application retries, or flow control. Put it behind the existing authenticated
peer boundary and use `hatCodec`/TLS when those properties are required. A
caller that needs server-side dispatch must validate the command and payload
against its own authorization and command registry before applying a mutation.

## Benchmark

Measured on AMD Ryzen 9 5950X, Linux/amd64, with
`make benchmark-t-u02-compact-protocol`:

| Operation | Compact | JSON baseline | Approximate result |
|---|---:|---:|---:|
| Marshal | `41.63-42.31 ns/op`, `80 B/op`, 1 alloc | `301.6-314.3 ns/op`, `208 B/op`, 2 allocs | `7.26x` faster, `2.60x` lower allocation bytes |
| Read/decode | `111.3-114.4 ns/op`, `144 B/op`, 3 allocs | `1407-1414 ns/op`, `352 B/op`, 7 allocs | `12.69x` faster, `2.44x` lower allocation bytes |

For the same `SETSTR`-shaped example, the compact frame is `69` bytes and the
JSON envelope is `130` bytes: `0.531x` the wire size, or about `1.88x` smaller.
These are codec microbenchmarks, not end-to-end network throughput; TLS,
compression, socket scheduling, command execution, and batching are outside
the measurement.

## Verification

```sh
make test-t-u02-compact-protocol
make test-t-u02-compact-package
make race-t-u02-compact-protocol
make vet-t-u02-compact-protocol
make measure-t-u02-wire-size
```
