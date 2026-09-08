# Persistent Shard Leases

`hatStorage.AcquirePersistentShardLease` provides explicit ownership for one
durable shard path. It is useful when two processes must never write the same
persistent shard at the same time, such as a primary and a recovery worker.

```go
lease, err := hatStorage.AcquirePersistentShardLease(store.Path(), "eu-west", "node-a")
if err != nil {
	if errors.Is(err, hatStorage.ErrPersistentShardLeaseHeld) {
		// Another process currently owns this shard. Retry or stay read-only.
	}
	return err
}
defer lease.Release()

if err := lease.Validate(); err != nil {
	return err
}
// Perform one durable mutation only after the fencing check.
```

Acquisition is non-blocking. The lock is local-filesystem scoped and is
released automatically by the operating system when the process exits. Every
successful acquisition increments a token that is persisted before the lease
is returned. A writer that stores only `store.Path()` and the token can call
`hatStorage.ValidatePersistentShardLeaseToken` immediately before a mutation.
`Renew` refreshes the durable liveness timestamp without changing the token;
it is an audit signal, not a wall-clock expiration mechanism.

The implementation keeps the advisory lock file separate from the state file.
State is written to a temporary file, synced, atomically renamed, and followed
by a directory sync. Shard IDs are SHA-256 encoded into filenames, so an
arbitrary shard ID cannot escape the lease directory. Lease and state files are
created with owner-only permissions. `InspectPersistentShardLease` reports the
last durable owner, token, timestamps, and whether the lock is currently held.

This is an ownership primitive, not a consensus protocol. It does not elect a
primary, replicate metadata, or make a remote filesystem's advisory-lock
semantics reliable. Multi-datacenter ownership still needs a consensus-backed
coordinator or the existing topology fencing protocol. Existing persistence
and single-process defaults are unchanged unless a caller opts into the lease.

For a node-wide restart generation that should not share a user shard ID, use
[`PersistentNodeEpoch`](PERSISTENT_NODE_EPOCHS.md).

## Verification

```sh
make test-persistent-shard-lease-local-clean
make benchmark-persistent-shard-lease-local-clean
```
