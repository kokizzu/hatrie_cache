# T-U28 Connection-Pool Lifecycle Hooks

`hat/hatPeer.ConnectionPool` can now publish physical connection lifecycle
events through the existing bounded `PeerLifecycleRegistry`.

## Events

- `PeerLifecycleConnected` is emitted when a new physical connection is
  admitted to the pool.
- `PeerLifecycleConnectFailed` is emitted once when a non-context dial attempt
  fails after the pool's retry policy completes.
- `PeerLifecycleDisconnected` is emitted when a physical connection is closed,
  including a terminal close error when one is returned.
- `PeerLifecycleShutdown` is emitted once when `Close` begins. A later close is
  idempotent; physical idle-connection disconnect events may follow shutdown.

Idle connection reuse does not emit connect or disconnect events. Hooks run
through the existing registry, which bounds history and recovers hook panics.
The pool never emits while holding its internal mutex, so a hook may inspect
pool state or request shutdown.

## Configuration

```go
registry, _ := hatPeer.NewPeerLifecycleRegistry(hatPeer.PeerLifecycleOptions{})
pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
    Dial:      dialPeer,
    Lifecycle: registry,
    PeerID:    "peer-west",
})
```

`Lifecycle: nil` is the default and preserves the old behavior. `PeerID` is
metadata copied into emitted events; it is not an authentication credential.

## Measured Cost

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X, with
`-benchtime=200ms`:

| Path | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Clean-base pool reuse | 47.76 | 0 | 0 |
| Pool reuse with lifecycle fields, nil registry | 49.65 | 0 | 0 |
| Opt-in connect, hook, handler-error, disconnect | 580.7 | 240 | 5 |

The default-path difference was about 4.0% in this run with no allocation
change. The hook-path cost is paid only for physical lifecycle transitions and
includes the bounded event registry and history update. Raw samples are in the
T-U28 section of `BENCHMARK.md`.
