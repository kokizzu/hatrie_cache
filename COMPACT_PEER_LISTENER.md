# Compact Peer Listener

`hatPeer.NewCompactPeerListener` turns the existing bidirectional compact
session into an explicitly started, bounded listener. It adds a fixed-size
handshake before `CompactPeerSession` is created:

- version negotiation rejects unsupported protocol versions;
- feature negotiation returns the intersection of client and server feature
  bits;
- configured payload compression is advertised through
  `CompactPeerFeaturePayloadCompression` and is enabled only after the peer
  negotiates that bit;
- a mandatory authorization callback decides whether the connection may start;
- `MaxConnections` reserves an admission slot before a handler goroutine is
  created;
- handshake deadlines bound slow or incomplete clients;
- `RequireTLS` rejects connections that are not TLS-wrapped;
- `Close` cancels accepted sessions and waits for them before `Done` closes.

## Secure Setup

Authorization is mandatory. The listener does not accept a nil policy and does
not put bearer tokens or certificate data into the compact protocol. For a
network deployment, wrap the listener with `tls.NewListener`, configure normal
certificate verification (usually mutual TLS), and inspect the TLS connection
state or verified peer certificate in `Authorize`:

```go
tlsListener := tls.NewListener(tcpListener, tlsConfig)
server, err := hatPeer.NewCompactPeerListener(tlsListener, hatPeer.CompactPeerListenerOptions{
    RequireTLS: true,
    Authorize: func(ctx context.Context, conn net.Conn, handshake hatPeer.CompactPeerHandshake) error {
        tlsConn := conn.(*tls.Conn)
        state := tlsConn.ConnectionState()
        if len(state.PeerCertificates) == 0 {
            return errors.New("peer certificate required")
        }
        return authorizeCertificate(ctx, state.PeerCertificates[0], handshake)
    },
    Session: hatPeer.CompactPeerSessionOptions{Handler: handleCompactRequest},
})
if err != nil {
    return err
}
go server.Serve(context.Background())
```

The callback must honor its context and must not retain the connection. TLS
certificate verification still belongs in `tls.Config`; `RequireTLS` only
enforces that the accepted connection exposes a TLS connection state.

## Client

The client performs the same handshake before creating its session:

```go
negotiated, err := hatPeer.PerformCompactPeerHandshake(ctx, conn, hatPeer.CompactPeerHandshakeOptions{
    Features: hatPeer.CompactPeerFeaturePayloadCompression,
})
if err != nil {
    return err
}
sessionOptions := hatPeer.CompactPeerSessionOptions{
    Protocol: hatPeer.CompactProtocolOptions{CompressPayloadsAbove: 1024},
}
sessionOptions = hatPeer.CompactPeerSessionOptionsForNegotiatedHandshake(sessionOptions, negotiated)
session, err := hatPeer.NewCompactPeerSession(conn, sessionOptions)
```

The listener automatically adds the compression feature to its advertised
server bits when `Session.Protocol.CompressPayloadsAbove` is positive. If the
client does not advertise the bit, the listener keeps that session plain. A
client using `PerformCompactPeerHandshake` must apply the returned handshake
with `CompactPeerSessionOptionsForNegotiatedHandshake` before constructing its
session. Direct sessions that skip the handshake retain their existing
compression settings.

Version zero selects version 1. The default handshake timeout is five seconds.
The default listener limit is 256 concurrent accepted connections. Both values
are bounded by constructor validation; set explicit values when the deployment
needs a different budget.

## Lifecycle And Metrics

`Serve` must be called explicitly, so adding this package does not open a port
or start a background server. `Close` stops accepting new connections and
cancels sessions created by the listener. `Stats` reports active connections,
accepted and admission-rejected connections, failed handshakes, authorization
failures, and successful negotiations.

The compact session continues to provide bounded in-flight requests and
out-of-order response correlation. The handshake does not implement consensus,
cluster membership, or application command authorization beyond the required
connection callback; those remain application policy.

## Measurement

Run:

```text
make benchmark-t-u02
```

Five samples on Linux/amd64 with an in-memory `net.Conn`:

| Samples | Median | Memory | Allocations |
| --- | ---: | ---: | ---: |
| 265.6, 263.8, 263.3, 261.2, 260.3 ns/op | 263.3 ns/op | 240 B/op | 7 allocs/op |

This cost is paid once per connection. Requests after the handshake use the
existing compact-session path. The tradeoff is an explicit pre-session round
trip and callback; the benefit is bounded admission and a clear authentication
and compatibility boundary instead of silently exposing a raw compact socket.
