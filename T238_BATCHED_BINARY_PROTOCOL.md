# T238 Batched Binary Protocol Requests

`hatPeer.CompactPeerSession.CallBatch` sends a bounded group of ordinary
compact-protocol request frames before waiting for responses. It returns the
responses in the same order as the input requests, even when handlers finish
out of order.

```go
requests := []hatPeer.CompactPeerBatchRequest{
	{Command: []byte("GET"), Payload: []byte("one")},
	{Command: []byte("GET"), Payload: []byte("two")},
}
responses, err := peer.CallBatch(ctx, requests)
```

The batch is not transactional. Each request is still an independent command
and the existing `MaxInFlight` limit applies. A context cancellation or any
response error removes all local pending entries; when request cancellation is
enabled, already-sent requests also receive the existing best-effort remote
cancellation signal. A canceled batch leaves the session usable for later
calls.

## Compatibility And Tradeoff

The wire format is unchanged: a batch is a sequence of normal compact frames,
not a new envelope. Existing peers therefore continue to handle each request
individually, and bandwidth per request is unchanged.

`CallBatch` is intended for independent requests where response latency or
server work dominates. It retains the ordered response slice and has more
temporary memory than one-at-a-time `Call` for tiny, local operations. Use
small batches when memory matters and keep `Call` for latency-free work that
does not benefit from overlap.

The focused tests cover ordered responses, empty batches, cancellation cleanup,
session reuse, race detection, and static analysis. Raw measurements are in
[BENCHMARK.md](BENCHMARK.md#t238-batched-binary-protocol-requests).
