# C235: Table-Part-Column Task Profiler

`SQLTaskProfiler` is a bounded, opt-in aggregation store for storage-task
telemetry. It groups caller-supplied read and write observations by operation,
table, physical part, and optional column. This complements the query profiler:
the query profiler explains a query's stages, while this profiler identifies
which physical parts and columns consumed the work.

## Usage

```go
profiler, err := hatSql.NewSQLTaskProfiler(hatSql.SQLTaskProfilerOptions{
	MaxEntries: 2048,
})
if err != nil {
	return err
}
defer profiler.Close()

captured, err := profiler.Record(hatSql.SQLTaskProfileSample{
	Operation:     hatSql.SQLTaskRead,
	Table:         "orders",
	Part:          "2026-10-06-0",
	Column:        "customer_id",
	Rows:          1000,
	Bytes:         64000,
	ElapsedNanos:  2 * time.Millisecond,
	AllocatedBytes: 4096,
})
if err != nil {
	return err
}
if !captured {
	// The bounded identity table is full; continue the data operation.
}

for _, profile := range profiler.Profiles() {
	log.Printf("%s %s/%s/%s: rows=%d bytes=%d duration=%s failures=%d",
		profile.Operation, profile.Table, profile.Part, profile.Column,
		profile.Rows, profile.Bytes, profile.ElapsedNanos, profile.Failures)
}
stats := profiler.Stats()
```

`Column` may be empty for a table-part-level task, such as part metadata or a
whole-part write. `AllocatedBytes`, `AllocatedObjects`, and `Failed` are
optional caller measurements; the profiler does not read process-wide runtime
statistics and does not add a background sampler.

## Limits And Semantics

- A zero limit selects the conservative default: 1024 identities and 256-byte
  table, part, and column names.
- Limits are bounded by a hard safety maximum of 1 MiB for each identifier and
  1 MiB identities in one profiler. Invalid negative or oversized limits are
  rejected at construction.
- Once `MaxEntries` distinct identities are retained, a new identity returns
  `captured == false` without changing the data operation. Existing identities
  continue to aggregate.
- `Profiles()` returns copies in deterministic operation/table/part/column
  order. `Stats()` reports retained entries, accepted records, and dropped
  records.
- Counters saturate at their maximum values rather than wrapping.
- `Record`, `Profiles`, `Stats`, and `Close` are concurrency-safe. `Close`
  rejects later records with `ErrSQLTaskProfilerClosed`.
- Operation, table, and part are required. Identifiers must be valid UTF-8,
  non-blank, and within their configured byte limits. Elapsed time cannot be
  negative.

The profiler intentionally does not hook every storage reader or writer
automatically. A storage implementation must call `Record` at the task
boundary it wants to measure. This keeps the default data path unchanged and
lets deployments choose the useful granularity.

## Cost

The focused benchmark compares an allocation-free direct map update with an
existing-key `SQLTaskProfiler.Record` update. On an AMD Ryzen 9 5950X, three
runs of each benchmark produced:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Direct map baseline | 59.15–59.75 | 0 | 0 |
| `SQLTaskProfiler.Record` | 88.95–93.52 | 0 | 0 |

The profiler costs about 1.50-1.58x the CPU time of the minimal map update,
with no measured heap allocation on the hot existing-key path. It is therefore
opt-in and intended for bounded diagnostics, not an unconditional addition to
every storage operation. Re-run `make benchmark-c235-task-profiler` on the
deployment CPU before using the numbers for capacity planning.
