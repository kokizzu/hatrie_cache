# C234 Automatic SQL Query Profiler

This is the ClickHouse-style query-profile idea adapted to `hatSql`: an
explicitly enabled profiler converts the executor's existing privacy-safe
stage metrics into bounded samples. It records stage elapsed time, rows, and
bytes, plus query-boundary allocation totals when requested.

## Usage

```go
profiler, err := hatSql.NewSQLQueryProfiler(hatSql.SQLQueryProfilerOptions{
    MaxQueries:         256,
    MaxSamplesPerQuery: 64,
    SampleEvery:        4,
    CaptureAllocations: false,
})
if err != nil {
    return err
}
defer profiler.Close()

result, err := hatSql.ExecuteSQLQueryParameters(
    ctx,
    "FROM CACHE('events') AS event SELECT event.kind",
    resolver,
    nil,
    hatSql.SQLQueryOptions{
        QueryID:      "events-query",
        QueryProfiler: profiler,
    },
)
if err != nil {
    return err
}
_ = result

profile, ok := profiler.Profile("events-query")
if ok {
    for _, sample := range profile.Samples {
        report(sample.Operator, sample.ElapsedTime, sample.Rows, sample.Bytes)
    }
    reportAllocations(profile.AllocatedBytes, profile.AllocationCount)
}
```

`QueryProfiler` is nil by default. With it enabled, stage profiling reuses the
existing execution metrics and remains bounded by the profiler limits. Query
IDs are generated when omitted, just like other observation features.

`SQLQueryProfileSample.ElapsedTime` is wall-clock elapsed time for one executor
stage. It is deliberately separate from the manually supplied `CPUTime` and
`BlockedTime` fields. `Bytes` prefers a stage's output bytes and falls back to
input bytes when output bytes are unavailable.

`CaptureAllocations` is false by default. When true, the query reads runtime
memory counters once at the start and once at the end. The profile reports
`AllocatedBytes`, `AllocationCount`, and end-of-query `HeapBytes`; these values
describe the whole query and are not attributed to individual stages.

## Tradeoff

Measured on an AMD Ryzen 9 5950X with `go test -benchmem -count=5
-benchtime=500ms`, using a two-row SQL query:

| Path | Median ns/op | B/op | Allocs/op | Relative to clean default |
| --- | ---: | ---: | ---: | ---: |
| Clean default | 7,748 | 4,592 | 19 | 1.00x |
| Existing observer | 9,362 | 5,316 | 30 | 1.21x |
| Automatic profiler, allocation capture off | 10,812 | 5,317 | 30 | 1.40x |
| Automatic profiler, allocation capture on | 58,877 | 5,332 | 30 | 7.60x |

The default profiler mode stays close to the existing observer path and adds
no steady-state allocations beyond it. Allocation capture is intentionally
opt-in because the two runtime memory snapshots dominate CPU time. Full raw
runs are recorded in [BENCHMARK.md](BENCHMARK.md#c234-automatic-query-profiler).
