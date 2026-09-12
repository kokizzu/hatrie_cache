# Compact Peer Session

`hatPeer.CompactPeerSession` is the opt-in connection adapter for the compact
Tarantool-inspired peer protocol. It combines `CompactProtocol` framing with
`CompactMultiplexer` correlation so one `net.Conn` can carry concurrent
requests, out-of-order responses, and inbound handler calls.

## Usage

```go
server, err := hatPeer.NewCompactPeerSession(conn, hatPeer.CompactPeerSessionOptions{
	MaxInFlight: 128,
	Handler: func(ctx context.Context, request hatPeer.CompactFrame) (hatPeer.CompactFrame, error) {
		return hatPeer.CompactFrame{
			Command: request.Command,
			Payload: handle(ctx, request.Payload),
		}, nil
	},
})
if err != nil {
	return err
}
defer server.Close()

response, err := server.Call(ctx, []byte("GET"), []byte("key"))
```

The constructor starts the reader loop immediately. `Handler` is optional for
a client-only session. A handler response is normalized to the request ID and
`CompactResponse` kind. Returning an error sends a bounded `CompactError`
frame, which the caller receives wrapped by `ErrCompactPeerRemote`.

## Bounds And Shutdown

- `MaxInFlight` defaults to `DefaultCompactPeerMaxInFlight` (`1024`) and is
  bounded by `1<<20`.
- The same bound limits outgoing pending calls and concurrently running
  inbound handlers.
- `CompactProtocolOptions` is passed through unchanged, so frame, command, and
  payload limits are checked before decoder allocation.
- A canceled call removes its local pending entry. A late response from that
  call is ignored, which avoids tearing down a healthy connection during a
  normal cancellation race.
- `Close` cancels handlers, closes the connection, and releases all pending
  calls. `Done` and `Err` expose terminal state.
- The session does not open listeners, perform service discovery, or choose an
  authentication mechanism. Existing HTTP, gRPC, and replication servers are
  unchanged because this adapter is only used when explicitly constructed.

## Security

The compact frame is bounded and rejects malformed, noncanonical, unknown, and
oversized input, but it is not an authentication or encryption layer. Put the
connection behind TLS or another authenticated private transport before
accepting untrusted peers. Validate commands and payloads in the handler, use
small protocol limits appropriate to the operation, and keep `MaxInFlight`
bounded. Do not expose a raw compact session on a public listener.

## Measurements

Measured on the repository's AMD Ryzen 9 5950X runner with three benchmark
runs (`-benchmem`). The session benchmark uses `net.Pipe`, an echo handler, a
single request per round trip, and no filesystem or network latency.

| Path | Time | Allocated | Allocations |
| --- | ---: | ---: | ---: |
| Compact marshal baseline | 40.3-41.0 ns/op | 80 B/op | 1/op |
| Compact read baseline | 100.7-101.7 ns/op | 144 B/op | 3/op |
| Compact peer session call | 5.39-5.49 us/op | 496-497 B/op | 9/op |
| JSON marshal baseline | 298.7-301.3 ns/op | 208 B/op | 2/op |
| JSON unmarshal baseline | 1.394-1.409 us/op | 352 B/op | 7/op |

The adapter intentionally costs more than direct codec calls because it adds
connection I/O, a reader goroutine, correlation bookkeeping, a handler
goroutine, and synchronized writes. Its benefit is a usable bounded
out-of-order transport: the compact wire remains about 1.88x smaller than the
JSON fixture (69 versus 130 bytes), while direct compact marshal/read remain
about 7.4x/13.8x faster and use fewer allocations than the JSON baselines.

## Verification

```text
make test-t-u02-session
make test-t-u02-session-package
make race-t-u02-session
make vet-t-u02-session
make benchmark-t-u02-session
```
