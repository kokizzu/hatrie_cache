# Persistent Node Epochs

`hatStorage.AcquirePersistentNodeEpoch` provides one durable generation for a
node process. It is useful when restart-sensitive writes need a value that
increases after every successful owner handoff, independently of individual
shard IDs.

```go
epoch, err := hatStorage.AcquirePersistentNodeEpoch(store.Path(), "node-a")
if err != nil {
	if errors.Is(err, hatStorage.ErrPersistentNodeEpochHeld) {
		// Another local process owns the node generation.
	}
	return err
}
defer epoch.Release()

if err := epoch.Validate(); err != nil {
	return err
}
generation := epoch.Epoch()
// Attach generation to writes and validate it immediately before persistence.
```

The first acquisition returns epoch `1`. After release or process exit, the
next acquisition returns a larger epoch. `Renew` refreshes the durable
liveness timestamp without changing the epoch. `InspectPersistentNodeEpoch`
reports the last owner, epoch, timestamps, and whether the lock is currently
held. A component that only has the storage path and epoch can call
`ValidatePersistentNodeEpoch` immediately before a durable mutation.

The implementation reuses the existing local persistent lease machinery but
stores its lock and state below the dedicated sibling path
`<storage-path>.node-epoch.leases`. This keeps node epochs separate from user
shard IDs. State is atomically published and synchronized before acquisition
returns, and the lock and state files are owner-only. The lock is advisory and
local-filesystem scoped; it is not a distributed timestamp oracle, consensus
protocol, or cross-datacenter lease. Use a consensus-backed coordinator for
global ordering and fencing across machines.

## Benchmark

Measured on the repository benchmark host (`AMD Ryzen 9 5950X`, Linux
amd64), five runs with `-benchmem`:

| Operation | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Persistent node epoch acquire/release | 1,723,340 | 4,937 | 47 |
| Direct persistent shard lease acquire/release | 1,574,790 | 4,599 | 45 |

Filesystem synchronization dominates both measurements and produced noisy
outliers. The node-epoch wrapper costs approximately 9% median time, 7% bytes,
and 4% allocations relative to direct shard-lease acquisition. It adds a
node-wide API and namespace; it does not claim a throughput improvement.

## Verification

```sh
make test-persistent-node-epoch-local-clean
make test-persistent-node-epoch-race-local-clean
make vet-persistent-node-epoch-local-clean
make benchmark-persistent-node-epoch-local-clean
```
