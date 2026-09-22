# C234: SQL Query Profiler Integration

## Adopted Idea

Attach a bounded query profiler directly to execution, as a compact analogue
of ClickHouse-style per-query profile records. A caller opts in with
`SQLQueryOptions.Profiler`; ordinary queries do not allocate or read runtime
memory statistics for this feature.

```go
profiler, err := hatSql.NewSQLQueryProfiler(hatSql.SQLQueryProfilerOptions{})
if err != nil {
    return err
}
defer profiler.Close()

result, err := hatSql.ExecuteSQLQueryParameters(ctx, source, resolver, nil,
    hatSql.SQLQueryOptions{
        QueryID:  "request-42",
        Profiler: profiler,
    })
```

The profiler also works with `ExecuteSQLQueryRows`; streamed execution records
a bounded `STREAM OUTPUT` stage with the completed row and byte counters.

## Recorded Data

For each completed operator event, the profiler records:

- `Operator`: the plan or stream operator name.
- `ElapsedTime`: wall-clock stage duration from `ElapsedNanos`. It is not
  mislabeled as CPU time; `CPUTime` remains available for callers that have
  actual CPU instrumentation.
- `Rows`: output rows for the stage.
- `Bytes`: saturating sum of known input and output bytes.
- `Timestamp`: the completion timestamp.

When the profiler is enabled, the executor also records a synthetic
`__query__` memory operator:

- `AllocatedBytes`: the process `runtime.MemStats.TotalAlloc` delta measured
  from query start to query completion.
- `HeapAllocBytes`: the process `runtime.MemStats.HeapAlloc` value at query
  completion. This is a current process-heap snapshot, not a query-local peak
  or retained-memory measurement.

The memory profile keeps saturating allocation totals and maximum observed heap
snapshots. It does not retain SQL text, parameters, stack traces, or row data.

## Bounds And Failure Behavior

The existing profiler limits remain in force: 256 query IDs, 64 stage samples
per query, and 64 memory operators per query by default, with hard safety
limits. Malformed, incomplete, or over-budget events are ignored by the
profiler and never fail the SQL query. Query IDs and operator names remain
bounded to 256 bytes.

Enabling the profiler intentionally requests execution metrics from native SQL
fast paths so that their plan stages are visible. A nil profiler keeps the
existing fast path and avoids both plan-observation work and `MemStats` reads.

## Measure Before Keeping

Command: `make benchmark-c234-query-profiler` (five samples, `-benchmem`,
Linux/amd64, AMD Ryzen 9 5950X).

| Path | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Before, no profiler | 260,922 | 67,593 | 400 |
| After, no profiler | 246,575 | 67,593 | 400 |
| After, existing observer | 320,744 | 89,775 | 1,171 |
| After, profiler enabled | 381,134 | 89,798 | 1,171 |

The default path stayed within normal run-to-run variance and retained the
same allocation profile. The enabled profiler is about 1.55x the default CPU
time and 1.19x the existing observer baseline in this workload, so it is not
enabled globally. Its retained state is bounded and its memory overhead was
effectively the observer baseline here, with 23 additional bytes per operation.

## Verification

The focused regression suite covers materialized and streamed execution,
stage bytes and duration, query allocation counters, incomplete events, and
the profiler-only path that previously lost native plan samples:

```text
make test-c234-query-profiler
```

The broader package, race, and vet checks are run before the feature is
committed and pushed.
