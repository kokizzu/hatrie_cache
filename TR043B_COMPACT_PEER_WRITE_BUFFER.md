# Reusable Compact Peer Frame Buffers

The compact protocol previously allocated a fresh encoded frame for every
request and response. That made the common uncompressed session path pay an
avoidable allocation even though writes are already serialized by the
session's write mutex.

## Implemented Design

`CompactProtocol.MarshalInto` appends a validated frame to a caller-provided
destination and has the same output and compression behavior as `Marshal`.
`Marshal` now delegates to it, preserving the existing API. Plain
`CompactPeerSession` writes reuse one buffer under the existing write mutex;
compressed writes continue through `CompactProtocol.Write` so gzip thresholds,
fallbacks, and frame flags are unchanged.

The session retains an encoded buffer only when the resulting frame is at most
64 KiB. Larger buffers are released immediately after the write, preventing a
single large request from permanently increasing per-session memory.

## Measurement

Five-sample local benchmarks on Linux/amd64 with the repository's AMD Ryzen 9
5950X runner:

| Path | Before | After | Improvement |
| --- | ---: | ---: | ---: |
| Plain `CompactProtocol` encoding | 55.99 ns/op, 80 B, 1 alloc | 21.43 ns/op, 0 B, 0 alloc | 2.61x faster; zero allocations |
| Plain session `Call` | 6.053 us/op, 512 B, 9 allocs | 6.004 us/op, 448 B, 7 allocs | 1.01x median; 12.5% lower B/op; 2 fewer allocs |

The session CPU difference is within `net.Pipe` scheduling noise, but the
allocation reduction is deterministic in the benchmark. The compressed
session path was covered by the existing round-trip test and retains its
previous behavior. See [BENCHMARK.md](BENCHMARK.md#tr-043b-reusable-compact-peer-frame-buffers)
for raw samples.
