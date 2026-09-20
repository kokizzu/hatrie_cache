# T-U28 Connection Pool Lifecycle

`hatPeer.ConnectionPool` now integrates with the existing opt-in
`PeerLifecycleRegistry`.

Configure `ConnectionPoolOptions.Lifecycle` and, optionally, `PeerID` to
receive these events:

- `PeerLifecycleConnected` after a dial succeeds.
- `PeerLifecycleDisconnected` after a pooled connection is physically closed.
  A close error is included when one is returned.
- `PeerLifecycleShutdown` once, after pool shutdown has drained active work.

Idle reuse does not emit another connected event, and a successful operation
does not emit a disconnect until the connection is actually discarded or the
pool closes. A nil lifecycle registry preserves the existing default path.

```go
lifecycle, err := hatPeer.NewPeerLifecycleRegistry(hatPeer.PeerLifecycleOptions{})
if err != nil {
	return err
}
pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
	Dial:      dialPeer,
	Lifecycle: lifecycle,
	PeerID:    "region-a-peer-1",
})
if err != nil {
	return err
}
defer pool.Close(context.Background())
```

Lifecycle hooks run synchronously on the operation that emits the event, so a
hook should enqueue slow work rather than block pool progress. The registry
still bounds hook count and retained history. Event peer IDs are capped at
256 bytes and terminal errors at 1,024 bytes before history retention, which
prevents untrusted close metadata from retaining unbounded strings.

This is an integration layer, not authentication, membership, reconnect
policy, or schema migration. Those remain caller-owned.

## Measurement

The baseline was measured before implementation on the same checkout with
`make benchmark-tu28-lifecycle` and five samples per benchmark.

| Path | Before | After | Tradeoff |
|---|---:|---:|---|
| Dial, handler error, and physical close | 493.6 ns/op, 240 B/op, 5 allocs/op | 659.1 ns/op, 240 B/op, 5 allocs/op | 1.34x from the first baseline; the same-run no-lifecycle control was 535.5 ns/op, or 1.23x, with no allocation increase |
| Reused idle connection | 48.93 ns/op, 0 B/op, 0 allocs/op | 48.0 ns/op, 0 B/op, 0 allocs/op | 0.98x latency, within benchmark noise, with no event per reuse |

The event-producing path pays for bounded history and callback delivery. The
feature remains disabled by default because it is observability/control-plane
functionality rather than a data-path optimization.
