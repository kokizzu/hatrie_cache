# Durable Connector Lifecycle State

`hatPipeline.ConnectorRegistry` is an in-memory lifecycle controller. The
optional M-U01 checkpoint API makes its bounded control-plane state portable
across process restarts without serializing connector implementations.

## API

```go
source, err := hatPipeline.NewConnectorRegistry(
	 hatPipeline.ConnectorRegistryOptions{HistoryLimit: 32},
)
if err != nil {
	 panic(err)
}
if err := source.Register("orders-eu", ordersConnector); err != nil {
	 panic(err)
}

payload, err := source.MarshalSnapshot()
if err != nil {
	 panic(err)
}

snapshot, err := hatPipeline.UnmarshalConnectorRegistrySnapshot(payload)
if err != nil {
	 panic(err)
}
restored, err := hatPipeline.NewConnectorRegistryFromSnapshot(snapshot,
	map[string]hatPipeline.Connector{
		"orders-eu": newOrdersConnector(),
	})
if err != nil {
	panic(err)
}
_ = restored
```

`ConnectorRegistry.SnapshotState` returns the detached typed form. A caller can
either persist the result through `MarshalSnapshot` or pass the typed value to
`NewConnectorRegistryFromSnapshot`. `RestoreSnapshot` provides the equivalent
operation for an already-created empty registry.

The connector map must contain exactly one non-nil fresh implementation for
each checkpointed ID. Connector implementations, goroutines, channels,
credentials, and callback state are never serialized. Restore does not invoke
`Start`, `Pause`, `Resume`, or `Stop`; the restored status is the last durable
observation, not proof that a process-local worker is currently running. The
caller should reconcile and explicitly start or stop work after restart.

Restore is atomic with respect to the destination registry: malformed data,
an occupied registry, or an incomplete/extra connector map leaves the
destination unchanged. The binary `HCS1` format is deterministic, versioned,
bounded, and protected by a CRC32C checksum. The current limits are 4,096
connectors, 262,144 total retained events, 64 MiB per payload, and 1 MiB per
connector ID or error string.

## Durability Guidance

The package returns bytes but does not choose a filesystem, object store, or
write policy. A production caller should write the payload to a temporary file,
flush it as required by its durability policy, and atomically rename it into
place. Keep the checkpoint separate from data backups: it contains connector
control-plane metadata and bounded history, not source offsets, table rows, or
secrets. Protect the resulting file with the same access controls as the
connector configuration.

Snapshots are point-in-time per connector and may observe different transition
generations across connectors when transitions race. Take the checkpoint at a
caller-owned lifecycle barrier when a cross-connector cut is required.

## Measurements

The five-run benchmark uses 64 connectors, history limit 8, and half of the
connectors paused. It runs with `GOMAXPROCS=1` and compares the binary codec to
Go's `encoding/json` codec for the same typed snapshot.

| Path | Median ns/op | B/op | Allocs/op | Wire bytes |
|---|---:|---:|---:|---:|
| Existing status-only `Snapshot` | 6,804 | 5,432 | 4 | n/a |
| New detached `SnapshotState` | 10,037 | 14,264 | 68 | n/a |
| Binary codec | 9,746 | 15,056 | 8 | 4,331 |
| JSON codec | 76,959 | 26,146 | 162 | 18,049 |
| Binary decode | 14,618 | 19,752 | 228 | 4,331 |
| JSON decode | 228,933 | 28,392 | 273 | 18,049 |

For codec-only work, binary encoding is 7.90x faster, 1.74x lower heap, 20.25x
fewer allocations, and 4.17x smaller on the wire than JSON. Binary decoding is
15.66x faster, 1.44x lower heap, and uses 1.20x fewer allocations. The new
full-state APIs are explicit opt-ins; the existing status-only `Snapshot`
path is unchanged.

Raw five-run `ns/op` samples:

```text
status: 7284, 6804, 6813, 6120, 6282
state: 9158, 9191, 10037, 10346, 10743
binary codec: 10241, 9378, 9823, 9746, 9337
json codec: 74031, 71883, 84444, 76959, 79453
binary decode: 14242, 14212, 15395, 15562, 14618
json decode: 244681, 245093, 227170, 228933, 227471
```
