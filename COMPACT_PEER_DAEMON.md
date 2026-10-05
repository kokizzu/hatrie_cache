# Compact Peer Daemon

`hatPeer.CompactPeerDaemon` is an opt-in listener-owning wrapper around the
bounded compact-peer handshake and session primitives. It binds an address,
optionally wraps it in TLS, negotiates the protocol, and starts a bounded
`CompactPeerSession` for each authenticated connection.

No daemon is created or started by default. The embedding service must call
`NewCompactPeerDaemon` and `Serve` explicitly.

## Mutual TLS

```go
daemon, err := hatPeer.NewCompactPeerDaemon(hatPeer.CompactPeerDaemonOptions{
	Network:                  "tcp",
	Address:                  "127.0.0.1:9001",
	TLSConfig:                serverTLSConfig,
	RequireTLS:               true,
	RequireClientCertificate: true,
	Listener: hatPeer.CompactPeerListenerOptions{
		Handshake: hatPeer.CompactPeerHandshakeOptions{
			Features: hatPeer.CompactPeerFeaturePayloadCompression,
		},
		Authorize: authorizePeerCertificate,
		Session: hatPeer.CompactPeerSessionOptions{
			MaxInFlight: 64,
			Handler:     handleCompactPeer,
		},
	},
})
if err != nil {
	return err
}
go daemon.Serve(ctx)
defer daemon.Close()
```

`RequireClientCertificate` is deliberately strict: the cloned server TLS
configuration must use `tls.RequireAndVerifyClientCert`. The `Authorize`
callback remains mandatory and is where the service maps a verified certificate
identity to a peer authorization policy. The daemon clones `TLSConfig`, so
later caller mutation cannot change an active listener's policy.

## Client Dial

```go
session, negotiated, err := daemon.Dial(ctx, hatPeer.CompactPeerDaemonDialOptions{
	TLSConfig: clientTLSConfig,
	Handshake: hatPeer.CompactPeerHandshakeOptions{
		Features: hatPeer.CompactPeerFeaturePayloadCompression,
	},
})
if err != nil {
	return err
}
defer session.Close()
response, err := session.Call(ctx, []byte("command"), payload)
_ = negotiated
```

`Dial` applies the caller context to TCP and TLS handshakes, then performs the
fixed-size version/feature handshake. Feature bits are intersected by the
server; unsupported compression is disabled before the session starts. The
session context and lifetime remain controlled by `Session.Context` and
`Close`.

## Policy and Limits

- `Address` is required; an empty `Network` selects `tcp`.
- A non-nil `TLSConfig`, `RequireTLS`, or `RequireClientCertificate` enables
  TLS policy. Missing TLS configuration is rejected before binding a socket.
- Plaintext is available only when TLS is not configured and `RequireTLS` is
  false. The authorization callback is still mandatory.
- `MaxConnections`, `MaxInFlight`, and compact protocol byte limits remain the
  admission and memory controls. A full connection slot is rejected rather
  than queued without bound.
- `Close` cancels active sessions and `Done` closes after all handlers exit.
- The daemon does not provide cluster membership, discovery, replay protection,
  mutation idempotency, or automatic failover. Those policies remain owned by
  the embedding service.

## Performance

Creating a fresh session is intentionally more expensive than reusing one: it
performs TCP setup, TLS, and compact-protocol negotiation. The five-sample
benchmark in [BENCHMARK.md](BENCHMARK.md#t-u02-authenticated-compact-peer-daemon)
measures that cost. Keep one session per peer and use its multiplexed calls for
the normal data path.
