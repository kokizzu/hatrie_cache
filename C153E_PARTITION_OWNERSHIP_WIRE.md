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
| marshal | 570.3 ns/op, 432 B/op, 2 allocs | 126.6 ns/op, 160 B/op, 1 alloc | 4.50x faster, 2.70x less allocation bytes |
| unmarshal | 3,212 ns/op, 608 B/op, 17 allocs | 220.0 ns/op, 136 B/op, 8 allocs | 14.60x faster, 4.47x less allocation bytes |
| payload | 266 bytes | 118 bytes | 2.25x smaller, 55.6% less bandwidth |

The binary path is substantially faster and smaller for this control-plane
message. It adds no background work and leaves the legacy JSON path available;
the remaining tradeoff is that consumers must use the exported codec rather
than relying on generic JSON serialization.

## Raw benchmark samples

```text
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2121344  568.6 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2109584  572.5 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2150533  560.0 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2098915  570.3 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/marshal-32  2084490  578.3 ns/op  432 B/op  2 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  364878  3195 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  370466  3244 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  380785  3219 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  366949  3212 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWireJSON/unmarshal-32  337366  3212 ns/op  608 B/op  17 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9559248  126.0 ns/op  160 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9248680  124.4 ns/op  160 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9131565  126.8 ns/op  160 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9407535  126.6 ns/op  160 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/marshal-32  9348489  127.1 ns/op  160 B/op  1 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5222091  228.5 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5362305  220.0 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5400908  219.8 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5526069  220.0 ns/op  136 B/op  8 allocs/op
BenchmarkC153ePartitionOwnershipVoteWire/unmarshal-32  5241472  221.0 ns/op  136 B/op  8 allocs/op
```
