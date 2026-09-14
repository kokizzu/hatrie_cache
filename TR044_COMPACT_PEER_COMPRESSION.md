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

The compact adapter accepts both plain and compressed frames, but it does not
perform a capability handshake. Enable compression only when every peer on
that connection understands the flag. Existing sessions remain plain because
the default is off.

Compression is useful when the link is bandwidth-bound and payloads repeat
well. It is not a general latency optimization: the measured 12.3 KiB
repetitive workload reduced wire bytes by about 83.7x, but added about 5.1x
write CPU and 7.8x read CPU. See the raw results in
[BENCHMARK.md](BENCHMARK.md#tr-044-selective-compact-peer-payload-compression).
