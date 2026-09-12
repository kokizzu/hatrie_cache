# Connector Lifecycle

`hatPipeline.ConnectorRegistry` is a small, explicit control plane for
Materialize-style source and sink lifecycles. It gives each registered
connector a stable ID, serialized lifecycle transitions, a bounded status
history, and a deterministic status snapshot. It does not start a goroutine,
open a network listener, or change the cache data path by itself.

## Example

```go
type source struct{}

func (source) Start(context.Context) error  { return nil }
func (source) Pause(context.Context) error  { return nil }
func (source) Resume(context.Context) error { return nil }
func (source) Stop(context.Context) error   { return nil }

registry, err := hatPipeline.NewConnectorRegistry(hatPipeline.ConnectorRegistryOptions{
	HistoryLimit: 32,
})
if err != nil {
	return err
}
defer registry.Close(context.Background())

if err := registry.Register("orders-eu", source{}); err != nil {
	return err
}
if err := registry.Start(ctx, "orders-eu"); err != nil {
	return err
}
status, ok := registry.Status("orders-eu")
if ok {
	log.Printf("connector=%s state=%s generation=%d", status.ID, status.State, status.Generation)
}
```

## States And Transitions

| Current state | Operation | Result |
|---|---|---|
| `created` | `Start` | `running` |
| `running` | `Pause` | `paused` |
| `paused` | `Resume` | `running` |
| `created`, `running`, `paused`, `failed` | `Stop` | `stopped` |
| `failed` | `Start` | `running` after a successful retry |

`Unregister(id)` drops a connector from the registry only while it is
`created` or `stopped`. Active connectors must be stopped first, which makes a
drop explicit and prevents silently abandoning connector work.

An invalid transition returns `ErrConnectorInvalidTransition` without calling
the connector. A successful `Stop` on an already stopped connector is
idempotent. A callback error, including a canceled context detected before the
callback, records a failure event and moves the connector to `failed`; a later
`Start` can recover it.

Calls for one connector are serialized. Different connector IDs may transition
concurrently. `Snapshot()` returns statuses sorted by ID. `Events(id)` returns a
copy of the newest retained events in chronological order, so callers cannot
mutate registry state. The default history limit is 32 events per connector;
the configurable limit is capped at 4096 and is preallocated per connector.

`Close(ctx)` marks the registry closed, stops registered connectors in ID order,
and is idempotent. Registration and public lifecycle operations after close
return `ErrConnectorRegistryClosed`. The registry is in-memory only; durable
restart state and connector-specific leases remain future work.

## Benchmark

Measured on AMD Ryzen 9 5950X, Linux/amd64, with
`make benchmark-mu01-connector-lifecycle`:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Registry: register plus four transitions | 541.5-546.3 | 528 | 5 |
| Direct connector: four callbacks | 7.424-7.506 | 0 | 0 |
| Snapshot: 128 connectors | 13,299-14,016 | 10,680 | 4 |

The registry is intentionally a control-plane feature, so its transition
coordination is roughly 73x the direct callback baseline. It is not intended
for per-record ingestion. The bounded event ring reduced the lifecycle
benchmark from `583.2 ns/op`, `672 B/op`, and `6 allocs/op` to approximately
`546.2 ns/op`, `528 B/op`, and `5 allocs/op` in the same local benchmark shape:
about `1.07x` faster, `1.27x` lower bytes, and `1.20x` fewer allocations. The
optimization changes only retained history storage and does not change the
public state contract.

Run the focused correctness test with:

```sh
make test-mu01-connector-lifecycle
```
