# Compact Peer Payload Compression

The opt-in `hatPeer` compact protocol can gzip large request and response
payloads. Configure both ends of a compact peer session with the same
compression policy:

```go
protocol := hatPeer.CompactProtocolOptions{
	CompressPayloadsAbove:       1024,
	MaxDecompressedPayloadBytes: 4 << 20,
}
session, err := hatPeer.NewCompactPeerSession(conn, hatPeer.CompactPeerSessionOptions{
	Protocol: protocol,
})
```

`CompressPayloadsAbove` is a byte threshold and `0` disables compression.
Payloads that do not become smaller are sent plain. The frame flag identifies
compressed payloads, and readers enforce `MaxDecompressedPayloadBytes` before
returning the inflated payload. This bounds memory use against compressed
payload expansion.

Direct compact sessions continue to accept both plain and compressed frames,
but they do not perform a capability handshake. For listener-managed sessions,
the listener advertises `CompactPeerFeaturePayloadCompression` automatically
when compression is configured and only enables compression after the peer
advertises the same bit. A peer that does not negotiate the bit receives plain
frames. Existing direct sessions remain unchanged and the default is still
off.

Client code that performs the handshake explicitly should apply the returned
features before creating its session:

```go
sessionOptions := hatPeer.CompactPeerSessionOptions{
    Protocol: hatPeer.CompactProtocolOptions{CompressPayloadsAbove: 1024},
}
negotiated, err := hatPeer.PerformCompactPeerHandshake(ctx, conn, hatPeer.CompactPeerHandshakeOptions{
    Features: hatPeer.CompactPeerFeaturePayloadCompression,
})
if err != nil {
    return err
}
sessionOptions = hatPeer.CompactPeerSessionOptionsForNegotiatedHandshake(sessionOptions, negotiated)
session, err := hatPeer.NewCompactPeerSession(conn, sessionOptions)
```

The capability bit is high-order so it does not collide with older callers'
low-order application feature bits. Unknown feature bits continue to be
ignored through the existing intersection negotiation.

Compression is useful when the link is bandwidth-bound and payloads repeat
well. It is not a general latency optimization: the measured 12.3 KiB
repetitive workload reduced wire bytes by about 83.7x, but added about 5.1x
write CPU and 7.8x read CPU. See the raw results in
[BENCHMARK.md](BENCHMARK.md#tr-044-selective-compact-peer-payload-compression).
