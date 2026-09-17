# M-U23 Cluster Query Admission

`hatSql.SQLClusterAdmission` is an opt-in scheduler for applications that run
multiple SQL workloads on one process. It gives each named cluster separate
serving and maintenance pools, reserving CPU units, optional memory bytes,
running slots, and queued slots. Existing SQL execution remains unchanged until
an application wraps it with `Acquire` or `Execute`.

## Defaults

`NewSQLClusterAdmission(SQLClusterAdmissionOptions{})` uses conservative local
defaults:

| Pool | CPU capacity | Running | Queue | Memory |
|---|---:|---:|---:|---|
| serving | `GOMAXPROCS` units | `GOMAXPROCS` | 64 | disabled |
| maintenance | 1 unit | 1 | 64 | disabled |

`CPUUnits: 0`, `MaxRunning: 0`, and `MaxQueued: 0` select those defaults.
`MemoryBytes: 0` deliberately disables memory accounting because the caller
must choose a meaningful estimate for its query workload. A positive memory
capacity turns memory into a hard admission limit. The controller accepts at
most 256 active clusters by default and 4,096 when explicitly configured.

The controller is not enabled by a global setting, does not create workers, and
does not change the existing query manager or namespace governor.

## Serving And Maintenance

The two classes have independent ledgers. A serving query cannot consume the
maintenance pool's CPU, memory, or running slots, and a maintenance job cannot
starve serving capacity. Waiters in one class are selected oldest-first among
requests that fit the currently available resources. A request larger than the
class capacity fails immediately rather than occupying a queue slot.

```go
admission, err := hatSql.NewSQLClusterAdmission(hatSql.SQLClusterAdmissionOptions{
	Default: hatSql.SQLClusterAdmissionPolicy{
		Serving: hatSql.SQLClusterAdmissionPool{
			CPUUnits: 4, MemoryBytes: 512 << 20, MaxRunning: 4, MaxQueued: 32,
		},
		Maintenance: hatSql.SQLClusterAdmissionPool{
			CPUUnits: 1, MemoryBytes: 256 << 20, MaxRunning: 1, MaxQueued: 8,
		},
	},
})
if err != nil {
	panic(err)
}
defer admission.Close()

request := hatSql.SQLClusterAdmissionRequest{
	Cluster: "orders-eu",
	Class:   hatSql.SQLClusterWorkServing,
	CPUUnits: 1, MemoryBytes: 64 << 20,
}
err = admission.Execute(ctx, request, func(ctx context.Context) error {
	_, err := hatSql.ExecuteSQLQueryParameters(ctx, source, resolver, parameters, options)
	return err
})
```

Use `SQLClusterWorkMaintenance` for backfills, compaction, projection refresh,
or other background work. A request with an empty class is treated as serving;
CPU zero means one requested unit and memory zero means no memory charge.

## Acquire And Release

`Acquire` returns a lease after capacity is reserved. Always release it with
`defer`; `Release` is idempotent and remains safe after controller shutdown.
Queued calls observe `context.Context` cancellation and remove their waiter
without consuming capacity. `Execute` is the convenience form and releases a
lease even when the callback returns an error.

`Close` rejects new calls and wakes queued callers with
`ErrSQLClusterAdmissionClosed`. Existing leases remain accounted for until
released, preventing a close race from making usage counters negative.

```go
lease, err := admission.Acquire(ctx, request)
if err != nil {
	return err
}
defer lease.Release()
return runQuery(ctx)
```

`Stats(cluster)` reports limits, CPU and memory usage, running count, and queue
depth for both classes. `Snapshot()` returns deterministic, sorted statistics
for active clusters; it is an observability view, not a persistence checkpoint.

## Validation And Security

Cluster names are required, bounded to 128 bytes, valid UTF-8, and cannot
contain control or format characters. CPU and memory requests cannot be
negative. Pool capacities, queue sizes, and configured cluster count are
bounded before any ledger is created. Invalid class names, oversized requests,
full queues, and closed controllers use stable sentinel errors. No SQL text,
parameters, or result data is retained by the scheduler.

## Cost And Scope

The scheduler is intended for query-scale work, where admission overhead is
small relative to execution. The paired microbenchmark compares a no-op direct
callback with the full `Execute` wrapper and reports CPU, bytes, and allocations
in [BENCHMARK.md](BENCHMARK.md#mu-023-cluster-query-admission). It is not a
claim that a scheduler accelerates a no-op function; use the existing direct
path when admission isolation is unnecessary.
