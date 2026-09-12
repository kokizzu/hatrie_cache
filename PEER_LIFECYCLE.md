# Peer Lifecycle Hooks

`hatPeer.PeerLifecycleRegistry` provides a bounded, in-process observation
point for peer connection and schema events. It is opt-in and has no package
global state.

## Registry

```go
registry, err := hatPeer.NewPeerLifecycleRegistry(hatPeer.PeerLifecycleOptions{
	MaxHooks:     64,
	HistoryLimit: 32,
})
if err != nil {
	return err
}

hookID, err := registry.Register(hatPeer.PeerLifecycleDisconnected,
	func(event hatPeer.PeerLifecycleEvent) {
		log.Printf("peer %s disconnected: %s", event.PeerID, event.Error)
	})
if err != nil {
	return err
}
defer registry.Unregister(hatPeer.PeerLifecycleDisconnected, hookID)
```

The zero-value options select `MaxHooks=64` and `HistoryLimit=32`. Both values
are bounded; the current hard ceiling is `4096`. A registry counts hooks across
all event kinds. `Snapshot()` returns the retained events in chronological
order, newest last. `Emit` timestamps events with UTC when `At` is zero and
normalizes supplied timestamps to UTC.

The available event kinds are:

- `PeerLifecycleConnected`
- `PeerLifecycleDisconnected`
- `PeerLifecycleShutdown`
- `PeerLifecycleSchemaReloaded`

Callbacks run in registration order, outside the registry mutex. They are
synchronous in the caller of `Emit`, so a callback should enqueue durable or
slow work rather than perform it inline. A panic from one callback is recovered
and does not prevent later callbacks from running.

## Compact Peer Sessions

Pass the registry and an application-level peer identifier to the existing
compact session adapter:

```go
session, err := hatPeer.NewCompactPeerSession(conn,
	hatPeer.CompactPeerSessionOptions{
		Lifecycle: registry,
		PeerID:    "region-apac/node-2",
		Handler:   handleRequest,
	})
if err != nil {
	return err
}
defer session.Close()
```

The session emits `connected` after it starts. The first terminal path emits
`disconnected` exactly once. An explicit `Close()` additionally emits
`shutdown`; a remote read failure or parent-context cancellation emits only
`disconnected`. Terminal notifications are dispatched from one bounded
termination goroutine so a slow hook cannot hold the protocol reader or block
`Close()` from closing the socket. Existing sessions with a nil `Lifecycle`
retain their previous behavior and do not create that goroutine.

This registry is an observation hook, not authentication, authorization,
membership, failover, or schema negotiation. Do not expose a raw compact peer
session to an untrusted network; wrap the connection in the deployment's
authenticated transport first. `PeerID` is application-supplied metadata and
is not trusted for access control.

## Measurement

On an AMD Ryzen 9 5950X, `make benchmark-t-u28-lifecycle` measured:

| Operation | Time | Allocations |
|---|---:|---:|
| Emit, no hooks | 50.35-52.18 ns/op | 0 B/op, 0 allocs/op |
| Emit, one hook | 52.13-55.21 ns/op | 0 B/op, 0 allocs/op |
| Snapshot, 32 retained events | 382.7-400.8 ns/op | 2,304 B/op, 1 alloc/op |

The one-hook path uses a direct dispatch fast path. Multiple hooks copy only
the bounded callback list before invoking it. Snapshot allocation is the
explicit cost of returning an independent slice and is not paid by session
connect/disconnect when no registry is configured.

Focused correctness, race, and vet checks are available through:

```sh
make test-t-u28-lifecycle
make test-t-u28-package
make race-t-u28-lifecycle
make vet-t-u28-lifecycle
```
