# SQL Query Profiler Samples

`hatSql` provides an opt-in bounded sample store for operator instrumentation.
It is intended for attaching a small CPU/blocking profile to an existing query
ID without retaining SQL text, parameters, or process-wide stack traces.

## Usage

Construct a profiler only when the service needs sampled operator diagnostics:

```go
profiler, err := hatSql.NewSQLQueryProfiler(hatSql.SQLQueryProfilerOptions{
    MaxQueries:         512,
    MaxSamplesPerQuery: 32,
    SampleEvery:        8,
})
if err != nil {
    return err
}
defer profiler.Close()

captured, err := profiler.Record(queryID, hatSql.SQLQueryProfileSample{
    Operator:    "scan",
    CPUTime:     140 * time.Microsecond,
    BlockedTime: 12 * time.Microsecond,
    Rows:        4096,
    Bytes:       128 << 10,
})
if err != nil {
    return err
}
_ = captured

profile, ok := profiler.Profile(queryID)
if ok {
    for _, sample := range profile.Samples {
        report(sample.Operator, sample.CPUTime, sample.BlockedTime)
    }
}
```

`Record` returns `captured=false` for samples skipped by `SampleEvery`. The
first submitted sample is retained, followed by every Nth submitted sample.
`Profile` returns an oldest-first copy, and `Profiles` returns all retained
query IDs in deterministic order. `Stats` exposes only bounded aggregate
counters for monitoring.

## Bounds And Defaults

- Profiling is off unless the application constructs and calls a profiler.
- `MaxQueries` defaults to 256 and `MaxSamplesPerQuery` defaults to 64.
- `SampleEvery` defaults to 1, retaining every submitted sample.
- The constructor rejects negative or excessive limits and caps the configured
  total retained sample capacity at 1,048,576 entries.
- Once a query reaches its sample limit, the oldest sample is overwritten.
- Once the query limit is reached, the least recently sampled query is evicted.
- Query IDs and operator names are bounded to 256 bytes each.
- `Close` stops future recording while preserving the last snapshot for reads.

CPU and blocking durations are supplied by the caller's instrumentation. The
profiler does not infer them, so it can be used with executor-specific timing
or external tracing measurements without changing ordinary SQL execution.

## Benchmark

The local test-first no-op baseline measured a median of 0.476 ns/op with zero
allocations. The post-implementation steady-state medians were:

| Operation | Median | Bytes/op | Allocs/op | Relative to no-op |
| --- | ---: | ---: | ---: | ---: |
| No-op sample accounting baseline | 0.484 ns | 0 | 0 | 1.00x |
| Captured `Record` | 27.27 ns | 0 | 0 | 56.4x slower |
| `SampleEvery=16` `Record` | 13.13 ns | 0 | 0 | 27.1x slower |
| 64-sample `Profile` copy | 715.9 ns | 4,864 | 1 | Inspection path |

The cost is paid only by callers that enable profiling and call `Record`.
Recording is allocation-free after a query's bounded ring is initialized;
snapshot copying allocates by design to protect the profiler's internal state.
See the raw samples in [BENCHMARK.md](BENCHMARK.md#ch-032-query-profiler-samples).
