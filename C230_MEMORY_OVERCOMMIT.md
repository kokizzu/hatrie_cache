# C230 Memory-Overcommit Query Admission

C230 adopts ClickHouse's memory-overcommit direction as an opt-in query
admission boundary. A query can wait for a bounded CPU/memory lease instead of
starting while the configured serving pool is full. The wait happens before
the source snapshot and read lock are acquired, so queued queries do not hold
source resources while they wait.

## Configuration

`SQLQueryOptions.ClusterAdmission` accepts the existing
`SQLClusterAdmission` controller. `ClusterAdmissionRequest` supplies the
cluster, work class, priority, CPU units, and estimated memory reservation:

```go
admission, err := hatSql.NewSQLClusterAdmission(hatSql.SQLClusterAdmissionOptions{
    Default: hatSql.SQLClusterAdmissionPolicy{
        Serving: hatSql.SQLClusterAdmissionPool{
            MemoryBytes: 8 << 30,
            MaxRunning:  32,
            MaxQueued:   128,
        },
        Maintenance: hatSql.SQLClusterAdmissionPool{
            MemoryBytes: 2 << 30,
            MaxRunning:  2,
            MaxQueued:   16,
        },
    },
})
if err != nil {
    return err
}

result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
    ClusterAdmission: admission,
    ClusterAdmissionRequest: hatSql.SQLClusterAdmissionRequest{
        Cluster:     "analytics",
        Class:       hatSql.SQLClusterWorkServing,
        CPUUnits:    1,
        MemoryBytes: 512 << 20,
    },
})
```

The same option applies to `ExecuteSQLQueryRows` and
`ExecuteSQLQueryPage`. `ClusterAdmission` is `nil` by default, so existing
queries retain immediate execution and do not allocate an admission lease.
Memory accounting is also disabled in an admission pool when its
`MemoryBytes` is zero.

## Waiting And Cancellation

- A request that fits but cannot start waits in the pool's bounded queue.
- `ctx.Done()` removes a queued request and returns the context error.
- `SQLQueryOptions.Timeout` also bounds time spent waiting for admission.
- A queue-full or permanently oversized request returns the existing typed
  admission error rather than waiting forever.
- A granted lease is released on success, parse/validation failure, callback
  failure, cancellation, or timeout.
- The reservation is an admission estimate; it does not replace the existing
  operator-level memory tracker or dynamically resize a running query.

## Tradeoff

The feature is safety and predictability work, not a single-query speedup.
Under no contention, the benchmark measured the direct path at a median
9,276 ns/op, 7,912 B/op, and 42 allocations, versus 9,651 ns/op, 8,032
B/op, and 44 allocations with admission enabled. That is 1.04x CPU, 1.02x
bytes, and 1.05x allocations in this run; the CPU difference is small and
should not be treated as a throughput claim. The low-level uncontended
acquire/release cost was a median 219.9 ns/op,
96 B/op, and one allocation. Under memory pressure, the benefit is avoiding
immediate rejection/cancellation and allowing a bounded queue to absorb short
bursts. Operators should choose queue limits and memory estimates explicitly so
waiting does not become unbounded latency.

## Verification

```sh
make test-c230-memory-overcommit
make race-c230-memory-overcommit
make vet-c230-memory-overcommit
make benchmark-c230-memory-overcommit
```
