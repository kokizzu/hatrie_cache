# T-U28: Connection Pool And Schema Lifecycle Triggers

Status: adopted as an opt-in `hatPeer.ConnectionPool` integration.

`PeerLifecycleRegistry` already provided bounded hooks and recent history, and
`CompactPeerSession` already emitted connection terminal events. This feature
extends the same contract to pooled physical connections and gives schema
owners an explicit notification point.

## Usage

Register only the event kinds a caller needs, then pass the registry to the
pool. `PeerID` is copied into every event.

```go
registry, err := hatPeer.NewPeerLifecycleRegistry(hatPeer.PeerLifecycleOptions{
	HistoryLimit: 64,
})
if err != nil {
	return err
}

_, err = registry.Register(hatPeer.PeerLifecycleDisconnected,
	func(event hatPeer.PeerLifecycleEvent) {
		logPeerDisconnect(event.PeerID, event.Error)
	})
if err != nil {
	return err
}

pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
	PeerID:    "orders-peer",
	Lifecycle: registry,
	Dial:      dialPeer,
})
if err != nil {
	return err
}
defer pool.Close(context.Background())
```

After a caller successfully applies a schema change, it can publish the
bounded trigger without coupling the pool to schema ownership:

```go
if err := reloadSchema(); err != nil {
	return err
}
if err := pool.NotifySchemaReloaded(); err != nil {
	return err
}
```

## Event semantics

- `PeerLifecycleConnected` is emitted once for each newly dialed physical
  connection admitted to the pool. Reusing an idle connection emits nothing.
- `PeerLifecycleDisconnected` is emitted after a physical connection is
  closed, including handler-error discard, idle-pool shutdown, and pool close.
  A close error is copied into a bounded 256-byte `Error` field.
- `PeerLifecycleShutdown` is emitted once after `ConnectionPool.Close` has
  stopped admissions and all active handlers have released their connections.
- `PeerLifecycleSchemaReloaded` is emitted only when the caller invokes
  `NotifySchemaReloaded`; the pool does not guess when an external schema
  manager has committed a change.
- Hooks execute outside the pool mutex. A hook may inspect pool state or start
  another operation without inheriting the pool's internal lock.
- A nil lifecycle registry preserves the existing pool behavior and hot path.
- Registry hook count and history remain bounded by
  `PeerLifecycleOptions` defaults and limits.

## Measurements

The raw samples are retained in `T028_BENCHMARK_BASELINE_RAW.txt` and
`T028_BENCHMARK_RAW.txt`. Runs used five samples, `-benchmem`,
`GOMAXPROCS=1`, and `-cpu=1` on Linux/amd64 with an AMD Ryzen 9 5950X.

| Workload | Baseline median | Feature median | Before/after ratio | Memory |
| --- | ---: | ---: | ---: | --- |
| Existing `ConnectionPool.Do` hot path | 47.41 ns/op | 45.97 ns/op | 1.03x | 0 B/op, 0 allocs/op in both |
| New `NotifySchemaReloaded` with one no-op hook | Not available before | 53.62 ns/op | New capability | 0 B/op, 0 allocs/op |

The default pool path therefore has no measured allocation or latency
regression. Lifecycle notification work is opt-in and only occurs at physical
connection or explicit schema lifecycle boundaries.

## Verification

```text
make t028-feature-test
make t028-feature-package
make t028-feature-compile
make t028-feature-race
make t028-feature-vet
make t028-feature-format
make t028-benchmark-run
```

The focused tests cover event order, peer ID and timestamps, physical close,
schema notification, opt-in behavior, nil/closed errors, and the existing pool
and peer lifecycle tests. Broader daemon/schema-manager wiring remains caller
owned and is the next integration boundary.
