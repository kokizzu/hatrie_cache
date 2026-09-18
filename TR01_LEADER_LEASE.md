# TR-01 Leader Lease and Fencing Primitive

This is a bounded, opt-in part of the Tarantool/Raft-inspired TR-01 idea. It
adds a small importable lease authority for callers that need a time-bounded
leader token and stale-writer fencing:

```go
leases, err := hatTopology.NewLeaderLeaseStore(hatTopology.LeaderLeaseOptions{})
if err != nil {
	return err
}

lease, err := leases.Acquire("shard-0", "node-a", 0) // default TTL
if err != nil {
	return err
}
if err := leases.Validate(lease.Name, lease.Holder, lease.Token); err != nil {
	return err
}
```

`Acquire`, `Renew`, `Release`, and `Validate` are serialized by one mutex.
Expired leases cannot be renewed or used, and every new acquisition receives a
strictly increasing token. A retry by the current holder is idempotent and
returns the existing token; `Renew` is the explicit expiry extension. The
default TTL is 15 seconds and the default maximum TTL is one minute. A custom
maximum below 15 seconds caps the zero-value TTL rather than allowing a longer
lease.

The store has no background goroutine and no persistence. A process restart
therefore loses all leases and must fail closed until a caller reacquires its
ownership. The caller must place the store behind a single control-plane
authority, quorum, or Raft implementation before using it for distributed
split-brain prevention. It is a reusable fencing primitive, not a Raft
implementation, membership protocol, vote transport, or automatic failover
system. The full TR-01 consensus integration remains open.

## Measurement

Five `-benchmem` samples ran on Linux/amd64 with an AMD Ryzen 9 5950X. The
control validates fixed holder, token, and expiry fields inline; the lease path
performs the synchronized map lookup and expiry check.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Inline holder/token/expiry control | 2.455; 2.302; 2.178; 2.170; 2.150 | 2.178 | 0 | 0 | 1.00x |
| `LeaderLeaseStore.Validate` | 23.87; 22.94; 23.37; 22.83; 23.76 | 23.37 | 0 | 0 | 10.73x |

The absolute cost is small but the relative cost is real. Keep validation at a
batch, ownership, or commit boundary; do not add this mutex/map lookup to every
hot data-path operation without measuring that workload. The default cache,
replication, and election paths remain unchanged.

## Verification

The tests cover acquisition, expiry, monotonic fencing, renewal and release
ownership, TTL validation, concurrent acquisition, and stale-token rejection.

```sh
make test-tr01-leader-lease
make race-tr01-leader-lease
make vet-tr01-leader-lease
make benchmark-tr01-leader-lease
```
