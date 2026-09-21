# C231 SQL Workload Groups

C231 adopts ClickHouse-style workload isolation for SQL execution. A caller can
reserve a bounded class-specific lease with independent CPU units, memory bytes,
running-query count, queue capacity, and optional priority aging.

The feature reuses the existing `SQLClusterAdmission` controller. It is opt-in:
`SQLQueryOptions.ClusterAdmission == nil` keeps the established execution path
unchanged.

## Usage

```go
admission, err := hatSql.NewSQLClusterAdmission(hatSql.SQLClusterAdmissionOptions{
	Default: hatSql.SQLClusterAdmissionPolicy{
		Serving: hatSql.SQLClusterAdmissionPool{
			CPUUnits:    8,
			MemoryBytes: 512 << 20,
			MaxRunning:  8,
			MaxQueued:   64,
		},
	},
})
if err != nil {
	return err
}

result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	ClusterAdmission: admission,
	ClusterAdmissionRequest: hatSql.SQLClusterAdmissionRequest{
		Cluster:       "interactive",
		Class:         hatSql.SQLClusterWorkServing,
		WorkloadClass: hatSql.SQLClusterWorkloadAdHoc,
		CPUUnits:      1,
		MemoryBytes:   64 << 20,
	},
})
```

Use `SQLClusterWorkMaintenance` for backfills, refreshes, or other background
work. Named policies in `SQLClusterAdmissionOptions.Clusters` can give separate
tenants or regions different capacities without sharing their queues.

`MemoryBytes` is a caller-supplied reservation for admission, not an RSS
measurement. Pair it with `SQLQueryOptions.MemoryOvercommit` when operators
also need a shared retained-memory wait queue. The reservation is acquired
before parsing/execution and released on normal completion, parse/validation
failure, timeout, and cancellation.

## Safety

- Nil admission is the default and preserves legacy behavior.
- Class queues are bounded; requests larger than a class capacity fail instead
  of waiting forever.
- Context cancellation removes a queued request.
- `Stats` and `Snapshot` expose running, queued, CPU, and memory state.
- A lease is released idempotently, including after controller shutdown.

## Measurement

The matched benchmark uses the same two-row materialized query for both paths.
Five `-benchmem` samples were run on Linux amd64 with an AMD Ryzen 9 5950X.
The workload-group variant has no contention; contention behavior is covered by
the focused tests.

| Variant | Median ns/op | B/op | allocs/op | Relative CPU | Heap delta | Allocation delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default admission disabled | 8,421 | 6,888 | 39 | 1.00x | baseline | baseline |
| Workload group enabled | 9,136 | 6,984 | 40 | 1.08x | +96 B | +1 |

This is a small opt-in admission cost in exchange for per-class concurrency and
memory isolation. It is not a throughput optimization.

Verification:

```text
make format-c231-workload-groups
make test-c231-workload-groups
make benchmark-c231-workload-groups
```
