# CH-U26 Operator Memory Profiles

`hatSql.SQLQueryProfiler` can retain a bounded memory profile alongside its
existing timing, row, and byte samples. The feature is opt-in: ordinary
`Record` callers do not create the memory-profile map or pay its accounting
work.

## Usage

```go
profiler, err := hatSql.NewSQLQueryProfiler(hatSql.SQLQueryProfilerOptions{
	MaxQueries:                 64,
	MaxMemoryOperatorsPerQuery: 32,
})
if err != nil {
	return err
}

accepted, err := profiler.RecordMemory("query-42", "scan:orders", hatSql.SQLQueryMemorySample{
	AllocatedBytes: 4096,
	PeakBytes:      1024,
	RetainedBytes:  768,
})
if err != nil || !accepted {
	return err
}

profile, ok := profiler.MemoryProfile("query-42")
if ok {
	// profile.Operators is sorted by operator name.
	_ = profile.Operators
}
```

`AllocatedBytes` is cumulative allocation work and is added with saturation at
`math.MaxUint64`. `PeakBytes` and `MaxRetainedBytes` keep the maximum observed
values. Each accepted observation increments the operator observation count.
The returned profile is an independent snapshot and can be serialized or
inspected without holding the profiler lock.

## Bounds and semantics

- `MaxMemoryOperatorsPerQuery` defaults to `64` and accepts values through the
  hard bound of `1024`.
- Memory profiles use the existing `MaxQueries` bound independently from the
  ordinary timing sample profiles. Oldest memory profiles are evicted when the
  query bound is reached.
- Once a query reaches its operator bound, new operator names are rejected with
  `(false, nil)` and counted in `DroppedObservations`; existing operators can
  continue receiving observations.
- `MemoryProfiles` returns query IDs and operator names in deterministic order.
- `SQLQueryProfilerStats` exposes memory observation, drop, and eviction
  counters without exposing query IDs or sample contents.
- `RecordMemory` validates query and operator names and returns the profiler's
  closed error after shutdown.

The profiler does not inspect process-wide runtime statistics, take heap
profiles, or infer ownership from goroutines. The SQL executor or another
caller-owned instrumenter supplies measurements at operator boundaries. This
keeps the API portable and privacy-safe while allowing a query engine to report
operator allocation, peak, and retained-byte behavior.

## Measurement

Five `-benchmem` samples were measured on Linux/amd64 with an AMD Ryzen 9
5950X. The ordinary path is the existing `Record` call; the memory path is a
warmed single-operator `RecordMemory` call.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Ordinary `Record` | 26.81; 27.44; 27.36; 27.54; 28.25 | 27.44 | 0 | 0 | 1.00x |
| Opt-in `RecordMemory` | 44.79; 43.68; 43.43; 42.78; 42.67 | 43.43 | 0 | 0 | 1.58x |

The explicit memory path costs about `1.58x` CPU in this isolated accounting
benchmark because it performs the additional operator aggregation and memory
map lookup. Both paths remain allocation-free after warm-up. The default
ordinary profiler path is unchanged unless a caller invokes `RecordMemory`.

Run the focused checks with:

```text
make test-chu26
make test-chu26-package
make race-chu26
make vet-chu26
make benchmark-chu26
```
