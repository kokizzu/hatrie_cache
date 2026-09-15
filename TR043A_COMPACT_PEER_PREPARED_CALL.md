# Prepared Compact Peer Calls

The compact peer package already had `CompactRequestTemplate` and a reusable
encoder, but `CompactPeerSession.Call` did not use them. Every ordinary call
went through the general frame marshaler and allocated a fresh request frame
buffer.

## Implemented Design

`NewCompactRequestTemplate` copies a command once. `CompactPeerSession.CallTemplate`
registers the request with the normal multiplexer, then uses
`CompactRequestTemplate.MarshalInto` and a per-session write buffer when the
session has no compression policy. The write mutex already serializes frames,
so the buffer does not introduce a new concurrent-access path.

When compression is configured, `CallTemplate` delegates to the existing
`CompactProtocol.Write` path. This preserves payload thresholds, gzip fallback,
frame flags, and decompression behavior. A plain request buffer is retained
only up to 64 KiB; larger requests release it immediately after a successful
write.

## Measurement

On the repository's AMD Ryzen 9 5950X runner with `net.Pipe`, an echo handler,
and one request per round trip, the five-sample medians were:

| Path | Median | Memory/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing `Call` | 6.053 us/op | 512 B | 9 | 1.00x |
| `CallTemplate` | 5.912 us/op | 480 B | 8 | 1.02x faster |

The CPU difference is modest because the benchmark is dominated by goroutine
and `net.Pipe` scheduling, but one allocation and 32 B/op are removed. The
standalone encoder benchmark remains about 2.5x faster with zero allocations
when its destination buffer is reused. See [BENCHMARK.md](BENCHMARK.md#tr-043a-prepared-compact-peer-calls)
for raw samples.
