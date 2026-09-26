# C153e Partition-Ownership Vote Wire Codec

C153e adds a bounded deterministic binary representation for one
`hatTopology.PartitionOwnershipConsensusVote`. It is an importable transport
primitive for HTTP, gRPC, or another caller-owned control-plane connection. It
does not open listeners, choose a transport, or change the existing JSON
representation.

## API

```go
payload, err := hatTopology.MarshalPartitionOwnershipConsensusVote(vote)
vote, err := hatTopology.UnmarshalPartitionOwnershipConsensusVote(payload)
```

The codec carries:

- shard ID, fencing token, primary, ordered replicas, and topology fingerprint;
- node ID and accepted/rejected state;
- optional C153d key ID and 32-byte HMAC-SHA256 signature.

Legacy unsigned votes remain valid. A key ID without a signature, malformed
metadata, non-canonical varints, unknown flags, trailing bytes, duplicate
replica names, and invalid whitespace are rejected. The decoder bounds the
whole payload at `64 KiB`, each string at `16 KiB`, and the replica count at
`1024` before allocating variable-length state. Decoded strings, replica
slices, and signatures own their memory.

The codec validates structure and bounds only. Authentication still requires
the existing `PartitionOwnershipConsensusAuthenticator.Verify` call after
decode.

## Measured result

Workload: one authenticated vote with two replicas, five benchmark samples,
AMD Ryzen 9 5950X, `go test -benchmem`.

| Operation | JSON baseline | Binary codec | Improvement |
|---|---:|---:|---:|
| marshal | 547.5 ns/op, 432 B/op, 2 allocs | 134.4 ns/op, 128 B/op, 1 alloc | 4.07x faster, 3.38x less allocation bytes |
| unmarshal | 3,125 ns/op, 608 B/op, 17 allocs | 238.0 ns/op, 136 B/op, 8 allocs | 13.13x faster, 4.47x less allocation bytes |
| payload | 266 bytes | 118 bytes | 2.25x smaller, 55.6% less bandwidth |

The binary path is substantially faster and smaller for this control-plane
message. It adds no background work and leaves the legacy JSON path available;
the remaining tradeoff is that consumers must use the exported codec rather
than relying on generic JSON serialization.

## Raw benchmark samples

```text
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2225984  547.5 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2175259  540.5 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2235372  546.2 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2177096  550.2 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2166837  556.7 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  368756  3106 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  322188  3125 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  368032  3141 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  393164  3183 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  396337  3124 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  8958972  134.7 ns/op  128 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9309651  134.4 ns/op  128 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9155068  133.4 ns/op  128 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  8872198  134.7 ns/op  128 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9138025  133.0 ns/op  128 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5083198  235.4 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5134428  238.0 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5095035  238.2 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5017592  235.4 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5073927  240.3 ns/op  136 B/op  8 allocs/op
```
