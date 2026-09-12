# Peer Transaction Streams

`hatPeer.CompactPeerStreamEndpoint` adds a bounded remote transaction
boundary to the compact peer protocol. One endpoint can open multiple logical
streams over one connection. Calls on different streams may run concurrently;
calls within one stream are serialized in order.

## Client

```go
endpoint, err := hatPeer.NewCompactPeerStreamEndpoint(conn,
	hatPeer.CompactPeerStreamOptions{})
if err != nil {
	return err
}
defer endpoint.Close()

stream, err := endpoint.OpenStream(ctx)
if err != nil {
	return err
}
defer stream.Close(ctx)

if _, err := stream.Call(ctx, []byte("PUT"), []byte("key=value")); err != nil {
	return err
}
if _, err := stream.Commit(ctx); err != nil {
	return err
}
```

`OpenStream` sends `begin`. `Call` sends an application command and payload.
`Commit` and `Rollback` are terminal operations. `Close(ctx)` rolls back an
open stream and is idempotent after a successful terminal operation. A
canceled or failed call leaves the stream open so the caller can retry or
rollback; a failed terminal operation also leaves it open.

## Server

The server handler owns the actual storage transaction, keyed by
`request.StreamID`:

```go
endpoint, err := hatPeer.NewCompactPeerStreamEndpoint(conn,
	hatPeer.CompactPeerStreamOptions{
		MaxStreams: 64,
		Handler: func(ctx context.Context, request hatPeer.CompactPeerStreamRequest) (hatPeer.CompactFrame, error) {
			switch request.Operation {
			case hatPeer.CompactPeerStreamBegin:
				return beginTransaction(ctx, request.StreamID)
			case hatPeer.CompactPeerStreamCall:
				return executeInTransaction(ctx, request.StreamID, request.Command, request.Payload)
			case hatPeer.CompactPeerStreamCommit:
				return commitTransaction(ctx, request.StreamID)
			case hatPeer.CompactPeerStreamRollback:
				return rollbackTransaction(ctx, request.StreamID)
			default:
				return hatPeer.CompactFrame{}, errors.New("unsupported stream operation")
			}
		},
	})
```

The library validates the operation order and removes server-side stream state
after a successful commit or rollback. It rejects duplicate begins, calls for
unknown streams, and new streams over `MaxStreams`. A nil handler is valid for
a client-only endpoint, but remote stream begins receive an explicit handler
error. Ordinary compact requests continue through the embedded session handler.

The stream envelope is versioned and uses bounded varints. Its internal
command marker is reserved; applications should use the endpoint for stream
calls rather than sending that marker as an ordinary compact command. Received
command and payload slices are borrowed for the duration of the handler and
must not be retained or mutated.

The zero `MaxStreams` value selects `64`, with a hard ceiling of `65,536` per
direction. The underlying compact session still enforces frame, command,
payload, and in-flight limits. This API does not provide authentication,
authorization, durable transaction recovery, or automatic rollback after a
process crash. Wrap the connection in the deployment's authenticated
transport, and make the handler's transaction recovery policy explicit.

## Measurement

On an AMD Ryzen 9 5950X, `make benchmark-t-u29-stream` measured three runs of a
small request/reply loop over `net.Pipe`:

| Path | Time | Memory | Allocations |
|---|---:|---:|---:|
| Raw compact session call | 5.411-6.196 us/op | 496-497 B/op | 9 allocs/op |
| Stream call | 5.433-5.848 us/op | 608 B/op | 10 allocs/op |

The stream boundary adds about `111-112 B/op` and one allocation for its
versioned envelope. It is a semantic feature, not a throughput optimization;
the extra cost buys multiplexed transaction identity and server-side state
validation. The benchmark keeps the existing raw session path as the control.

Focused checks:

```sh
make test-t-u29-stream
make test-t-u29-package
make race-t-u29-stream
make vet-t-u29-stream
```
