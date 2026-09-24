# C235 bounded table-part task profiler

`hatSql.SQLTaskProfiler` is an opt-in collector for storage adapters that need
to understand read and write work by table, immutable part, and column. It is
caller-instrumented because `hatSql` cannot infer a storage adapter's part
identity from an arbitrary `SQLSourceResolver`.

## Usage

```go
profiler, err := hatSql.NewSQLTaskProfiler(hatSql.SQLTaskProfilerOptions{
	MaxEntries: 4096,
})
if err != nil {
	return err
}

_, err = profiler.Record(hatSql.SQLTaskProfileRecord{
	Table:     "events",
	Part:      "part-0001",
	Column:    "user_id",
	Operation: hatSql.SQLTaskRead,
	Rows:      1024,
	Bytes:     8192,
	Duration:  2 * time.Millisecond,
})
if err != nil {
	return err
}

profiles := profiler.Profiles()
stats := profiler.Stats()
profiler.Close()
```

Each profile is keyed by `(table, part, column, operation)`. `Rows`, `Bytes`,
`Duration`, and `Tasks` aggregate only that operation. `ReadTasks` and
`WriteTasks` make exporters explicit. `Profiles` returns an independent,
deterministically sorted snapshot; `Stats` exposes only bounded counters.

## Safety and defaults

- No profiler is created or consulted by default SQL execution.
- `MaxEntries == 0` selects `DefaultSQLTaskProfilerMaxEntries` (4,096).
- The hard bound is 65,536 entries, and identity labels are limited to 256
  bytes each.
- A new identity at capacity evicts the least-recently observed identity;
  `Stats().EvictedEntryCount` reports the loss.
- The collector retains labels and counters only. It does not retain row
  values, query text, payload bytes, or user-provided secrets.
- Counters saturate instead of wrapping. `Close` is idempotent and leaves the
  last snapshot available while rejecting new records.

Use a short-lived profiler or export and recreate it when periodic windows are
needed. Do not put raw user input into `Table`, `Part`, or `Column`; use stable
catalog identifiers to avoid high-cardinality labels.

## Measured cost

On the repository's AMD Ryzen 9 5950X host, five benchmark samples with
`-benchmem` reported:

| Path | Median-ish result | Allocations |
| --- | ---: | ---: |
| Existing `SQLQueryProfiler.Record` control, one operator label | 21.39 ns/op | 0 B/op, 0 allocs/op |
| `SQLTaskProfiler.Record`, existing four-field identity | 60.18 ns/op | 0 B/op, 0 allocs/op |

The task profiler therefore adds about 2.8x CPU over the narrower control in
this microbenchmark, while retaining zero allocations on the hot path. The
comparison is not a query-speed claim: C235 provides bounded storage-task
visibility that the query profiler does not. Leave it unconfigured when that
visibility is not needed.

The benchmark is reproducible with `make benchmark-c235`; the focused tests
are run with `make test-c235`.
