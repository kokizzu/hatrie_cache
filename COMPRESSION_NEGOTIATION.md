# Compression Level Negotiation

`hat/hatCodec.CompressionLevelPolicy` chooses a compressed-block flate level
that both server and client support. It is a policy helper; callers decide how
to carry the client range in their handshake or request.

```go
policy, err := hatCodec.NewCompressionLevelPolicy(
	flate.BestSpeed,
	flate.HuffmanOnly,
	flate.BestCompression,
)
preferred := flate.BestCompression
level, err := policy.Negotiate(flate.HuffmanOnly, flate.BestSpeed, &preferred)
```

The result is clamped to the intersection of the server and client ranges. A
nil preferred level uses the server default. Invalid ranges and incompatible
ranges return `ErrCompressionLevelPolicyInvalid`.

`flate.NoCompression` is intentionally rejected because the existing
`CompressedBlockOptions` format uses level zero as an omitted value and maps it
to `flate.BestSpeed`. This keeps negotiation compatible with the current codec
without changing wire framing or silently selecting a different level.
