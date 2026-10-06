# M-U35 Snapshot Barrier

`hatPipeline.SnapshotBarrier` is an opt-in readiness gate for a group of
source snapshots, arrangements, or other maintained objects that must all be
ready before dependent work starts.

It provides a common blocking contract without coupling the pipeline package
to a particular source or SQL executor:

- `Wait(ctx)` blocks until every named prerequisite is ready;
- `MarkReady` is idempotent and wakes waiters when the set becomes complete;
- `MarkFailed` fails the current epoch and wakes every waiter;
- `Reset` starts a new epoch after the caller has repaired or replaced the
  failed prerequisite;
- `Close` wakes waiters and permanently rejects new state changes; and
- `Status` returns deterministic pending/ready names for operator reporting.

## Usage

```go
barrier, err := hatPipeline.NewSnapshotBarrier(hatPipeline.SnapshotBarrierOptions{
	Prerequisites: []string{"orders", "customers"},
})
if err != nil {
	return err
}

go func() {
	if err := loadSnapshot(ctx, "orders"); err != nil {
		_ = barrier.MarkFailed("orders", err)
		return
	}
	_ = barrier.MarkReady("orders")
}()

if err := barrier.Wait(ctx); err != nil {
	return err
}
// All configured snapshots are ready at this point.
```

The prerequisite set is fixed at construction time. This makes readiness
deterministic and prevents a late registration from silently changing the
meaning of an already-running wait. An empty set is ready immediately, which
allows callers to disable a barrier through configuration without a separate
code path.

`MarkFailed` retains the error for the current epoch. The error is returned
through `Wait` wrapped with `ErrSnapshotBarrierFailed`; `Status` exposes only
the error text, so callers should not put credentials or other secrets in
failure messages. `Reset` clears all readiness and starts the next epoch.

## Bounds and safety

The default limit is 256 prerequisites and the hard limit is 4096. Names are
UTF-8 and bounded to 128 bytes by default, with a 1024-byte hard maximum.
Waits are context-aware and use a close-and-replace notification channel, so
there is no polling timer or goroutine owned by the barrier. `Status` returns
detached sorted slices and is intended for monitoring rather than a query hot
path.

The barrier does not persist snapshots, perform source I/O, or automatically
admit queries. A caller composes it with its existing snapshot store and
execution admission policy. This keeps the default pipeline behavior and
allocation profile unchanged for users that do not construct one.

## Benchmark

The benchmark ran on the repository's AMD Ryzen 9 5950X host with
`-benchtime=200ms -count=5`; values below are medians of five runs.

| Operation | Workload | ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Direct readiness check | Boolean baseline | 0.250 | 0 | 0 |
| `Wait` | Already-ready one-prerequisite barrier | 5.002 | 0 | 0 |
| `Status` | Already-ready two-prerequisite barrier | 111.3 | 80 | 3 |

The synchronization check costs about 4.75 ns over a raw boolean in this
microbenchmark, while the ready wait remains allocation-free. Status copying
is intentionally more expensive because it returns detached operator data;
callers should sample it rather than invoke it for every query.
