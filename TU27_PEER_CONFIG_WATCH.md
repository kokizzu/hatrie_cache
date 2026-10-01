# T-U27 Peer Configuration Watch

`hatTopology.ConfigWatchLog` now has an opt-in binary peer protocol for
replaying or waiting on configuration changes through an existing
`hatPeer.CompactPeerSession`. It is transport-neutral: it does not open a
listener, start a server, or change the default HTTP/gRPC/HTTP2 paths.

## Contract

- Command: `hatTopology.ConfigWatchPeerCommand` (`_hat.topology.config_watch.v1`).
- Operations: `ConfigWatchPeerClient.Read` and `Wait`.
- Optional `Prefix` filters events by key prefix on the server, so unrelated
  changes do not wake a prefix wait.
- The cursor is the last delivered matching event version. Reuse it after a
  reconnect.
- A retained-history gap returns `*hatTopology.ConfigWatchGapError`; reload a
  fresh configuration snapshot, then resume from its current version.
- Requests and responses use bounded version-1 binary payloads. The default
  response bound is 8 MiB; the hard client limit is 64 MiB.

## Server Wiring

The compact session already supplies handshake authentication, request
correlation, bounded in-flight handlers, and optional request cancellation.
Route the stable command to the log:

```go
handler := func(ctx context.Context, frame hatPeer.CompactFrame) (hatPeer.CompactFrame, error) {
	if string(frame.Command) != hatTopology.ConfigWatchPeerCommand {
		return hatPeer.CompactFrame{}, errors.New("unsupported peer command")
	}
	payload, err := log.HandleConfigWatchPeerRequestForPrincipal(ctx, authenticatedPrincipal, frame.Payload)
	if err != nil {
		return hatPeer.CompactFrame{}, err
	}
	return hatPeer.CompactFrame{Kind: hatPeer.CompactResponse, Payload: payload}, nil
}
```

Use the existing authenticated `CompactPeerListener` or session options with
this handler. The `ConfigWatchLog` authorizer still runs for every remote
read/wait. `HandleConfigWatchPeerRequestForPrincipal` binds the wire
`Principal` to the authenticated peer identity; do not use the unbound helper
when the transport has an authenticated identity available.

## Client Wiring

The client is stateless and can be recreated after reconnecting:

```go
watch, err := hatTopology.NewConfigWatchPeerClient(hatTopology.ConfigWatchPeerClientOptions{
	Principal: "ops",
	Prefix:    "feature/",
	Call: func(ctx context.Context, command, payload []byte) ([]byte, error) {
		frame, err := session.Call(ctx, command, payload)
		return frame.Payload, err
	},
})
events, cursor, err := watch.Wait(ctx, lastCursor, 64)
```

Store `cursor` with the consumer's own checkpoint. If the peer reports a
history gap, perform the application's snapshot/reconciliation flow instead
of silently skipping changes. `Wait` forwards cancellation; enabling compact
session request cancellation lets the remote handler stop promptly.

## Safety And Limits

Malformed frames are rejected before allocation-heavy decoding. Event source,
key, value, prefix, event-count, and response-size bounds are checked on both
sides. Remote authorization failures return non-sensitive error codes; the
server's detailed error remains local. The log retains its existing bounded
history and value-copy isolation.

See [CONFIG_WATCH.md](CONFIG_WATCH.md) for the local log contract and
[COMPACT_PEER_PROTOCOL.md](COMPACT_PEER_PROTOCOL.md) for the underlying
transport.
