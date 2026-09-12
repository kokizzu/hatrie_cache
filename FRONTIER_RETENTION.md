# As-Of Frontier Retention

`hatPipeline.FrontierRetentionRegistry` tracks the oldest active historical
read for each named frontier. It gives compaction code a bounded, explicit
answer to how far history may be removed.

## Acquire A Lease

```go
retention, err := hatPipeline.NewFrontierRetentionRegistry(
	frontiers,
	hatPipeline.FrontierRetentionOptions{MaxLeases: 1024},
)
if err != nil {
	return err
}

lease, err := retention.Acquire("orders-apac", asOf)
if err != nil {
	return err
}
defer retention.Release(lease)

// Read the historical view at lease.AsOf.
```

An `asOf` timestamp must be at least the frontier's current `Lower` and no
greater than `Upper`. A timestamp below `Lower` is already expired; a timestamp
above `Upper` is not available yet. Acquisition checks the frontier both before
and after taking the retention lock, so obvious races with frontier progress
are rejected.

## Compaction Boundary

```go
boundary, err := retention.SafeCompactionBefore("orders-apac")
if err != nil {
	return err
}

if requestedBoundary <= boundary {
	compactHistoryStrictlyBefore(requestedBoundary)
}
```

`SafeCompactionBefore` is the minimum of the frontier's completed `Lower` and
the oldest active lease. With no leases it returns `Lower`. A compactor that
removes history strictly before `boundary` must require
`CanCompactBefore(frontierID, boundary)` or apply the same `boundary <= safe`
check. A successful commit/rollback of a lease removes its protection; failed
or canceled reads leave the lease active until the caller releases it.

`Snapshot` and `SnapshotAll` expose lease counts, the oldest requested
timestamp, and the safe boundary for operator metrics. Results are sorted by
frontier ID. The zero maximum selects `1024` active leases, with a hard ceiling
of `1,048,576`; no background goroutine or unbounded queue is created.

The registry does not compact data or persist leases. A process restart must
recreate leases for still-live consumers before enabling historical compaction.
It also does not make an external storage rewrite atomic with lease acquisition;
storage engines that require that guarantee should hold their compaction lock
around the boundary check and deletion plan. No network input or secret is
accepted by this API.

## Measurement

On an AMD Ryzen 9 5950X, `make benchmark-m-u33-retention` measured:

| Operation | Time | Memory |
|---|---:|---:|
| Safe boundary, no lease | 36.92-40.28 ns/op | 0 B/op, 0 allocs/op |
| Safe boundary, one lease | 41.14-45.07 ns/op | 0 B/op, 0 allocs/op |
| Acquire then release | 296.1-306.4 ns/op | 416 B/op, 3 allocs/op |

The per-compaction check adds no allocation. Lease churn pays for the bounded
per-frontier lease map and is expected to be much less frequent than row-level
compaction checks.

Focused checks:

```sh
make test-m-u33-retention
make test-m-u33-package
make race-m-u33-retention
make vet-m-u33-retention
```
