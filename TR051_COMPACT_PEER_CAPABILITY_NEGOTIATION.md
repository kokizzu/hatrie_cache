# Compact Peer Capability Negotiation

The compact peer protocol already had a fixed handshake with a version and
feature mask, but compression was configured independently on each session.
That allowed a listener to emit compressed frame flags even when the peer had
not declared support for them.

## Implemented Design

`CompactPeerFeaturePayloadCompression` reserves one high-order feature bit for
the existing gzip payload flag. `NewCompactPeerListener` advertises that bit
when its session has a positive `CompressPayloadsAbove` threshold. The
handshake still selects the intersection of client and server bits. Before a
listener creates a session, it applies the selected features and sets the
compression threshold to zero when the bit was not negotiated.

Clients using the explicit handshake API apply the same rule with
`CompactPeerSessionOptionsForNegotiatedHandshake`. Direct sessions created
without a handshake are unchanged, preserving the existing opt-in behavior.

## Operational Effect

The change prevents incompatible compressed frames without changing the
handshake wire format: the request remains 9 bytes and the response remains 10
bytes. It adds no per-request feature negotiation and no additional session
allocation. The five-sample benchmark measured `306.7 ns/op`, `240 B/op`, and
`7 allocs/op` for the existing mask versus `296.2 ns/op`, `240 B/op`, and
`7 allocs/op` with the compression bit; the CPU difference is benchmark noise.
See [BENCHMARK.md](BENCHMARK.md#tr-051-compact-peer-capability-negotiation) for
raw samples.

Compression remains opt-in because TR-044 measured substantial CPU and
temporary memory costs in exchange for lower bandwidth. This feature changes
only compatibility and activation safety; it does not change that compression
tradeoff.
