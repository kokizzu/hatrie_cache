# Compact Request Cancellation

`hatPeer.CompactPeerSession` can optionally propagate a caller cancellation or
deadline to the remote handler. Enable `EnableRequestCancellation` on both
ends of a compact connection:

```go
server, err := hatPeer.NewCompactPeerSession(conn, hatPeer.CompactPeerSessionOptions{
	EnableRequestCancellation: true,
	Handler: func(ctx context.Context, request hatPeer.CompactFrame) (hatPeer.CompactFrame, error) {
		return handleRequest(ctx, request)
	},
})
```

After a request has been sent, cancellation removes the local pending waiter
and sends the reserved `_hat.peer.cancel.v1` request with the original request
ID. The receiving session cancels the child context for that request. Handlers
must observe `ctx.Done()` or return from context-aware work for the remote work
to stop.

The feature is opt-in and uses the existing compact request envelope. The
default remains unchanged: no cancellation frame is sent, and handlers use the
session context directly. The cancellation message is best effort. A response
that races with cancellation is ignored by the canceled local waiter, and a
missing remote handler is not surfaced as a new error to the caller that has
already received its context error.

## Cost And Measurement

Measured on Linux amd64, AMD Ryzen 9 5950X, with five benchmark samples:

| Workload | Median time | Heap/op | Allocs/op | Meaning |
|---|---:|---:|---:|---|
| Existing normal call, `origin/master` before T240 | 5.414 us | 496 B | 9 | Clean control |
| Normal call after T240, option off | 5.266 us | 512 B | 9 | No measurable latency regression in this sample; the session object is 16 B larger |
| Canceled call, local-only control | 7.304 us | 722 B | 14 | Caller stops waiting and test handler is released locally |
| Canceled call, remote propagation enabled | 9.863 us | 1,058 B | 21 | About 1.35x the cancellation-path time, plus one wire frame and remote context state |

The enabled cancellation path costs more CPU and temporary memory because it
does work that the local-only path deliberately omits. Its benefit is
operational: a remote handler that honors its context can stop database work,
scans, sleeps, or stream transactions instead of continuing after the caller
has gone away. It does not make ordinary calls faster and is not enabled by
default.

Run the focused test and benchmark with:

```text
make test-t240
make race-t240
make vet-t240
make benchmark-t240
```
