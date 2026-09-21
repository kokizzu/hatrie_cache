# C234 Query Stage Profiler

The SQL query profiler now supports bounded per-query stage metrics. A caller
can record stages such as `scan`, `filter`, `aggregate`, or `project` and read
their cumulative CPU, blocked time, row, byte, and memory metrics.

## Usage

```go
profiler, err := hatSql.NewSQLQueryProfiler(hatSql.SQLQueryProfilerOptions{
	MaxQueries:       256,
	MaxStagesPerQuery: 64,
})
if err != nil {
	panic(err)
}

accepted, err := profiler.RecordStage("query-1", hatSql.SQLQueryStageSample{
	Stage:          "scan",
	CPUTime:        2 * time.Millisecond,
	Rows:           1000,
	Bytes:          65536,
	AllocatedBytes: 4096,
	PeakBytes:      2048,
	RetainedBytes:  1024,
})
if err != nil || !accepted {
	// The query or stage was rejected by validation or a configured bound.
}

profile, ok := profiler.StageProfile("query-1")
```

Recording is explicit: existing callers that only use `Record` do not create
stage maps or pay stage-observation cost. The default per-query stage bound is
64 and the hard maximum is 1024. A query can contain at most the configured
number of distinct stages; additional stages are rejected without mutating the
profile and are counted in `DroppedStageObservationCount`. Stage snapshots are
returned in deterministic stage-name order.

Invalid limits, empty query IDs, empty stage names, negative durations, and
closed profilers return errors. Query eviction and dropped-stage counters are
exposed through `SQLQueryProfilerStats`.

## Benchmark

Commands:

```text
make benchmark-c234-baseline
make benchmark-c234-stage-profiler
```

Five 200 ms samples on an AMD Ryzen 9 5950X:

| Path | Samples (ns/op) | Median | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Existing `Record` before C234 | 33.73, 31.70, 33.87, 32.35, 30.53 | 32.35 ns | 0 | 0 |
| Existing `Record` after C234 | 28.41, 25.77, 27.49, 27.29, 27.39 | 27.39 ns | 0 | 0 |
| `RecordStage` | 51.30, 55.81, 62.01, 57.00, 48.97 | 55.81 ns | 0 | 0 |
| `StageProfile` with 64 stages | 9289, 8892, 10327, 9816, 9352 | 9352 ns | 5528 | 4 |

The ordinary profiler path shows no regression in this run. Stage recording is
approximately 2.04x the ordinary observation path because it maintains a
bounded stage map and aggregates additional fields, while retaining zero
allocations. Snapshotting is intentionally a read/monitoring operation rather
than a hot-path operation.
