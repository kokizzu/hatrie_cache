# CH-G42: Per-Operator SQL Memory Tracking

Hatrie SQL now has an opt-in per-operator working-set tracker inspired by
ClickHouse memory governance. It records the estimated retained bytes at the
existing bounded accounting points and can reject an operator that exceeds its
own configured limit.

## Default

The feature is off by default. `SQLQueryOptions.OperatorMemoryTracker == nil`
keeps the existing execution path and does not allocate tracker state or take
tracker locks.

## Usage

```go
tracker, err := hatSql.NewSQLOperatorMemoryTracker(64 << 20)
if err != nil {
    return err
}
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
    OperatorMemoryTracker: tracker,
})
if err != nil {
    return err
}
for _, operator := range tracker.Snapshot() {
    log.Printf("sql operator=%s peak_bytes=%d samples=%d", operator.Operator, operator.PeakBytes, operator.Samples)
}
_ = result
```

`NewSQLOperatorMemoryTracker(0)` records without enforcing a limit. A positive
limit is applied independently to each operator. Exceeding it returns an error
matching `ErrSQLOperatorMemoryLimit`; the query does not return a partial
result. `Snapshot` is deterministic and reports `CurrentBytes == 0` after the
query has returned.

The tracker is safe to inspect concurrently, but use one tracker per query so
the current working set has a single owner. Peak and rejection counters remain
available after release.

## Covered Operators

The first slice uses existing byte estimates and covers:

- `GROUP BY`: grouped rows and aggregate state frontier.
- `SORT`: materialized `ORDER BY` rows, including the bounded external-sort
  admission point.
- `SET`: membership input for non-`ALL` `UNION`, `INTERSECT`, and `EXCEPT`.

Join, window, and scan-specific allocations are intentionally not inferred
from unrelated row counts yet. They should be added only at their own precise
retention boundaries rather than reported as misleading estimates.

## Cost Measurement

Measured with:

```text
make benchmark-chg42
```

Workload: two-row `GROUP BY` followed by `ORDER BY`, Go benchmark with
`-benchtime=10000x -count=3` on Linux/amd64.

| Mode | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| default | 10,111-13,022 | 8,264 | 55 |
| fresh opt-in tracker | 14,009-14,923 | 9,478-9,479 | 82 |

Using the median samples, the opt-in path costs about 35% CPU, 1,214 bytes,
and 27 allocations for this small workload because the benchmark creates a new
tracker for every query.
There is no default-path cost from the tracker. This is a governance and
diagnostic feature, not a throughput optimization; callers should enable it
for bounded workloads, admission control, or diagnostics where that cost is
worth the visibility.
