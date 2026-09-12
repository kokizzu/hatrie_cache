# Frontier Registry

`hatPipeline.FrontierRegistry` is an importable Materialize-style progress
contract for named dataflow objects. Each object publishes a lower frontier
that is safe for completed reads and an upper frontier that bounds the latest
known work. Both values are monotone, and `lower <= upper` is enforced.

This is separate from `hatPartition.PartitionFrontierBarrier`: the barrier
waits for a fixed numeric set of partitions, while this registry exposes
named object snapshots and can be used by external consumers.

## Example

```go
frontiers, err := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{
	MaxObjects: 1024,
})
if err != nil {
	return err
}
defer frontiers.Close()

if err := frontiers.Register("orders-eu"); err != nil {
	return err
}
if err := frontiers.Advance("orders-eu", 120, 128); err != nil {
	return err
}

if err := frontiers.WaitUntil(ctx, "orders-eu", 120); err != nil {
	return err
}
snapshot, ok := frontiers.Snapshot("orders-eu")
if ok {
	log.Printf("%s lower=%d upper=%d", snapshot.ID, snapshot.Lower, snapshot.Upper)
}
```

`Advance` with equal values is an idempotent no-op. A lower or upper regression
returns `ErrFrontierRegression`; a lower value above upper returns
`ErrFrontierOrderInvalid`. `WaitUntil` wakes when lower reaches the requested
target, when the object is removed/registry is closed, or when its context is
canceled. Multiple waiters share one lazy notification channel.

`SnapshotAll` is sorted by ID. The default registry limit is 1,024 objects and
the maximum is 1,048,576. `Unregister` removes one object and wakes its
waiters. `Close` rejects later registration/advance calls and wakes every
waiter. State is in-memory; durable frontier snapshots and restart recovery
remain separate work.

## Performance

Measured on AMD Ryzen 9 5950X, Linux/amd64, with
`make benchmark-m-u09-frontier`:

| Operation | Before lazy notifications | After lazy notifications | Result |
|---|---:|---:|---:|
| Advance with no waiter | `95.0-97.2 ns/op`, `112 B/op`, 1 alloc | `55.9-56.7 ns/op`, `0 B/op`, 0 allocs | `1.70x` faster; allocation removed |
| Wait already ready | `10.96-11.57 ns/op`, 0 B/op, 0 allocs | `10.74-11.40 ns/op`, 0 B/op, 0 allocs | unchanged within noise |
| Snapshot all, 128 objects | `14.19-14.30 us/op`, `10,680 B/op`, 4 allocs | `13.98-14.29 us/op`, `10,680 B/op`, 4 allocs | unchanged within noise |

The common advance path allocates no notification state unless a consumer is
actually waiting. Snapshot allocation is proportional to the requested output
and is not retained by the registry.

## Verification

```sh
make test-m-u09-frontier
make test-m-u09-package
make race-m-u09-frontier
make vet-m-u09-frontier
make benchmark-m-u09-frontier
```
