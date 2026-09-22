# C235: Read/Write Task Profiler

## Adopted Idea

ClickHouse-style physical I/O visibility is useful when a logical query is
slow because one table part or column is unusually expensive. `hatSql` now
provides an opt-in bounded aggregator keyed by table, physical part, column,
and direction. Storage adapters call it around their actual read/write task;
the generic SQL resolver does not guess physical boundaries or infer bytes.

```go
profiler, err := hatSql.NewSQLReadWriteProfiler(
    hatSql.SQLReadWriteProfilerOptions{},
)
if err != nil {
    return err
}
defer profiler.Close()

profiler.RecordRead("events", "part-2026-09-23-01", "score",
    hatSql.SQLReadWriteTaskSample{
        Rows:  4096,
        Bytes: 64 << 10,
    })
profiler.RecordWrite("events", "part-2026-09-23-01", "score",
    hatSql.SQLReadWriteTaskSample{
        Rows:  256,
        Bytes: 4 << 10,
    })

for _, aggregate := range profiler.Snapshot() {
    report(aggregate.Table, aggregate.Part, aggregate.Column,
        aggregate.Operation, aggregate.Rows, aggregate.Bytes,
        aggregate.Duration)
}
```

`RecordRead` and `RecordWrite` aggregate task count, logical rows, physical
bytes, duration, and the latest task timestamp independently. `Snapshot` is
sorted deterministically by table, part, column, and operation. Numeric totals
use saturating arithmetic so malformed or very large counters cannot wrap into
small values.

## Bounds And Defaults

- Constructing the profiler is required; there is no global/default instance.
- `MaxEntries` defaults to 1,024 aggregate entries and is capped at 65,536.
- Each table, part, and column label is trimmed and limited to 256 bytes.
- When the entry bound is reached, the least recently recorded aggregate is
  evicted before the new key is accepted; `Stats` exposes the eviction count.
- The profiler has no goroutine and does not retain row values or payloads.
- A closed profiler rejects new records but keeps its last snapshot available.
- Invalid labels and negative durations are rejected; callers can treat
  profiler errors as diagnostics without failing the underlying I/O task.

The API is intentionally caller-supplied. A storage adapter knows whether a
read was a physical part/column operation and can report the exact transferred
bytes, while a generic SQL query only knows logical rows and cannot safely
invent those values.

## Measure Before Keeping

Command: `make benchmark-c235-read-write-profiler` (five samples,
`-benchmem`, Linux/amd64, AMD Ryzen 9 5950X).

| Path | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| No-op counter baseline | 0.4743 | 0 | 0 |
| Repeated bounded read record | 97.89 | 0 | 0 |

The repeated-key path is allocation-free after the first aggregate entry is
created. Its roughly 206x CPU cost over a no-op is the explicit opt-in cost of
normalization, locking, aggregation, and timestamp handling; it does not
change default SQL or storage behavior.

## Verification

Focused correctness and concurrency checks:

```text
make test-c235-read-write-profiler
make race-c235-read-write-profiler
make vet-c235-read-write-profiler
```
