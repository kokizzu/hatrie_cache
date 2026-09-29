# Strict Leader Write Fencing

T203 adds `hatTopology.LeaderLeaseStore.WithFence` for the write boundary that
must not accept a stale leader after failover. The method validates the holder,
token, and expiry, then invokes the supplied write callback while the same lease
store mutex is held. A lease release, expiry transition, or new acquisition
cannot interleave between validation and the protected write when they use the
same authority.

## Usage

```go
err := leases.WithFence(lease.Name, lease.Holder, lease.Token,
    func(current hatTopology.LeaderLease) error {
        return applyLeaderWrite(current.Token, record)
    })
```

The callback is not invoked for a missing, expired, wrong-holder, or stale
token. Callback errors are returned unchanged and do not revoke the lease. The
callback receives a detached `LeaderLease` value, not mutable authority state.

Keep the callback short and do not call lease-store methods from it; it runs
under the store mutex by design. The embedding service still owns transport
authentication, storage commit, and distributed authority placement. An
in-memory store cannot by itself solve cross-process split brain; use one
serialized authority or consensus-backed lease state for a distributed writer
fence.

`LeaderLeaseStore.Validate` remains available for observation and existing
callers. `WithFence` is opt-in and does not change lease acquisition, renewal,
release, or the default write path.

## Measurement

The parent `BenchmarkTR01LeaderLeaseValidation` was measured before the change
with five 200 ms samples on an AMD Ryzen 9 5950X, linux/amd64:

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing `Validate` baseline | 23.63, 21.23, 20.99, 21.59, 23.10 | 21.59 | 0 | 0 |
| `WithFence` atomic callback | 25.23, 25.58, 27.80, 28.05, 24.77 | 25.58 | 0 | 0 |
| `Validate` plus write callback control | 21.36, 21.38, 21.23, 21.36, 21.15 | 21.36 | 0 | 0 |

The atomic write boundary costs about `1.20x` CPU or `4.22 ns/op` versus the
racy validation-plus-write control, with no additional memory or allocations.
That is the explicit tradeoff for closing the validation-to-write race;
existing validation callers are unchanged.

Reproduce it with:

```text
make benchmark-t203
```
