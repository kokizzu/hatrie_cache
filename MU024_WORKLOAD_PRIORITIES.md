# M-U24 Workload Classes And Priorities

`hatSql.SQLClusterAdmission` now accepts shared workload metadata for source,
compute, sink, and ad-hoc work. The metadata is useful across both serving and
maintenance pools, while `SQLClusterWorkClass` still selects the resource pool.
The feature is opt-in; existing SQL execution and legacy admission requests are
unchanged.

## Request API

Use `WorkloadClass` to identify the producer of work and `Priority` to select
its relative urgency. Higher priorities run first when requests fit the same
cluster/class pool.

```go
request := hatSql.SQLClusterAdmissionRequest{
	Cluster:       "region-eu",
	Class:         hatSql.SQLClusterWorkServing,
	WorkloadClass: hatSql.SQLClusterWorkloadAdHoc,
	Priority:      90,
	CPUUnits:      1,
}

err := admission.Execute(ctx, request, func(ctx context.Context) error {
	return runQuery(ctx)
})
```

The supported workload classes are:

| Class | Intended producer |
|---|---|
| `default` | Existing callers that do not classify work |
| `source` | Source ingestion and CDC work |
| `compute` | Maintained projections and dataflow computation |
| `sink` | External sink delivery |
| `ad_hoc` | Interactive or ad-hoc queries |

An empty workload class normalizes to `default`. Priority defaults to zero and
must be between zero and `MaxSQLClusterAdmissionPriority` (currently `100`).
Unknown workload classes and out-of-range priorities are rejected as invalid.

## Scheduling

Admission first enforces the existing CPU, memory, running-count, and queue
bounds. Among fitting queued requests, the controller chooses the highest
effective priority. Effective priority is the explicit priority plus bounded
queue age, capped at the maximum priority. A queued waiter gains one age unit
each time another waiter is granted. Equal effective priorities retain enqueue
order.

This provides a bounded starvation guarantee: a priority-zero waiter that fits
the pool is selected no later than `MaxSQLClusterAdmissionPriority+1` competing
maximum-priority grants, assuming it remains queued and its context is alive.
Serving and maintenance continue to use independent resource pools.

When no positive-priority waiter is queued, the controller uses the original
first-fitting FIFO scan. This keeps legacy/default queues inexpensive while
allowing all workload classes to use the same API when priorities are needed.

## Compatibility And Cost

Requests written before M-U24 have zero values for the new fields and normalize
to `default` at priority zero. The admission controller is still opt-in, so
callers that do not use `SQLClusterAdmission` pay no scheduling cost. The
priority path adds one integer of age state per queued waiter and scans the
bounded waiter list when a positive-priority waiter is present. Selection does
not allocate.

The benchmark is a selector-only measurement, not query latency. It uses a
32-waiter, one-CPU pool on Linux/amd64 with `GOMAXPROCS=1`, `-benchtime=500ms`,
and `-count=5`:

| Path | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Previous FIFO first-fit control | 1.009 | 0 | 0 | 1.00x |
| Current legacy FIFO fastpath | 3.166 | 0 | 0 | 3.14x slower |
| Current positive-priority selector | 97.44 | 0 | 0 | 96.57x slower |

The absolute cost is bounded queue bookkeeping, not execution of the admitted
work. The zero-allocation legacy fastpath and the opt-in boundary are why this
tradeoff is retained rather than applied to the normal SQL path.

## Reproducing

```text
make test-mu024-workload-priority
make benchmark-mu024-workload-priority-baseline
make benchmark-mu024-workload-priority
make race-mu024-workload-priority
make vet-mu024-workload-priority
```

Raw benchmark output is recorded in
[BENCHMARK.md](BENCHMARK.md#mu-024-workload-classes-and-priorities).
