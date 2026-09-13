# MZ-26: Exact-Frontier Subscription Snapshot Export

`QuerySubscriptions.ExportSnapshotsAt` provides a bootstrap/export operation
for all active query subscriptions at one logical frontier. It is useful when
a downstream consumer needs a consistent historical starting point instead of
replaying every live update from each subscription's current snapshot.

## Usage

```go
snapshots, err := registry.ExportSnapshotsAt(
	context.Background(),
	frontier,
	historicalResolver,
	QueryOptions{},
)
if err != nil {
	return err
}
for _, snapshot := range snapshots {
	// Snapshots are ordered by subscription ID and contain full query results.
	consume(snapshot.ID, snapshot.Frontier, snapshot.Result)
}
```

The resolver must implement `HistoricalSourceResolver`. Every query is
evaluated at the requested frontier, results are detached from subscription
state, and the returned list is sorted by subscription ID. `AsOf` and `UpTo`
are enforced: a frontier before `AsOf` or after `UpTo` is rejected. A snapshot
at `UpTo` is marked `Complete`.

The operation does not advance a subscription revision, publish an update, or
close a subscription. `Revision` reports the live revision observed when the
export was prepared; the exported `Frontier` identifies the point-in-time
query result. An evaluation or resolver error aborts the export and returns no
partial list.

## Cost And Safety

This is intentionally on-demand, not part of `Subscribe`, `NotifyChanged`, or
`Snapshot`. It re-executes each query and clones each result. Callers should
use the existing live `Snapshot()` method for ordinary reads and reserve
`ExportSnapshotsAt` for downstream bootstrap, checkpoint, or recovery work.

The API does not add a network listener, bypass resolver validation, or retain
additional frontier history. The caller controls access to the resolver and
should apply its normal authorization before exporting query results.

## Measurement

Command:

```text
make benchmark-mz026
```

Linux `amd64`, AMD Ryzen 9 5950X, five `-benchmem` samples, eight one-row
subscriptions:

| Operation | Median CPU | Allocated memory | Allocations |
| --- | ---: | ---: | ---: |
| Existing current `Snapshot()` loop | 2,445 ns/op | 3,008 B/op | 32 allocs/op |
| Exact-frontier export | 43,133 ns/op | 45,944 B/op | 252 allocs/op |

Exact export is `17.64x` the CPU and `15.27x` the allocated bytes of the
current-snapshot loop because it evaluates eight queries at a requested
historical point. That cost is isolated to the explicit export operation; the
live subscription and ordinary snapshot paths are unchanged. Raw samples are
recorded in [BENCHMARK.md](BENCHMARK.md#mz-26-exact-frontier-subscription-snapshot-export).
