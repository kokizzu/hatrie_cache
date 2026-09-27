# MZ-037 SQL Incremental Top-K

This adds an opt-in SQL compiler bridge to the existing exact weighted
`hatSql.IncrementalTopK` operator. It is useful for a CDC or differential
source that already has a stable key and wants SQL projection/filter semantics
without rebuilding and sorting the complete result after every update.

## Usage

```go
compiled, err := hatSql.CompileSQLQuery(
    "FROM CACHE('items') AS src " +
        "SELECT src.id, src.score " +
        "ORDER BY src.score DESC LIMIT 2",
)
if err != nil {
    return err
}
operator, err := compiled.CompileIncrementalTopK()
if err != nil {
    return err
}

_, err = operator.Apply([]hatSql.DifferentialRow{
    {Key: "a", Diff: 1, Row: hatSql.Row{"id": "a", "score": int64(10)}},
    {Key: "b", Diff: 1, Row: hatSql.Row{"id": "b", "score": int64(20)}},
})
if err != nil {
    return err
}
current := operator.Snapshot()
```

`DifferentialRow.Key` is the stable source identity. Positive updates for a
new key carry the source row; retractions may omit it because the operator
retains the source image. `Apply` validates the whole batch before publishing
state, and returns only changes to the bounded result. `Snapshot` returns
projected rows in SQL order.

## Supported Shape

- One `CACHE` or `VALUES` source.
- Scalar `SELECT` expressions and an optional scalar `WHERE` expression.
- Exactly one binary-collated scalar `ORDER BY` expression.
- A finite `LIMIT`, with no `OFFSET` or `LIMIT WITH TIES`.
- No joins, grouping, `DISTINCT`, windows, CTEs, unions, custom functions, or
  materialized filtering stages.
- Null order values are rejected with `ErrSQLIncrementalTopKOrderNull` rather
  than silently using ordering semantics that could differ from SQL.

Unsupported shapes return `ErrSQLIncrementalTopKUnsupported`; generic SQL
execution is unchanged. Automatic subscription planner selection is not yet
implemented.

## Benchmark

Command:

```text
make benchmark-mz037-sql-topk
```

The workload seeds 64 rows, then repeatedly replaces the same retained key
with a delete-plus-insert batch. Five benchmark samples were collected on the
same machine. Medians are calculated from the raw samples below.

| Path | Median ns/op | B/op | allocs/op | Relative CPU | Relative bytes |
|---|---:|---:|---:|---:|---:|
| Direct `IncrementalTopK` | 1,807 | 2,160 | 13 | 1.00x | 1.00x |
| SQL adapter after fast path | 2,695 | 2,832 | 17 | 1.49x | 1.31x |
| SQL adapter before fast path | 3,018 | 3,248 | 20 | 1.67x | 1.50x |

The adapter is intentionally a capability bridge, not a replacement for the
lower-level operator. Its SQL evaluation and projection cost is measurable;
the replacement fast path reduces adapter CPU by about 1.12x, bytes by about
1.15x, and allocations by about 1.18x versus the first implementation.

Raw result after the fast path:

```text
BenchmarkMZ037SQLIncrementalTopKDirect-32      573687  1804 ns/op  2160 B/op 13 allocs/op
BenchmarkMZ037SQLIncrementalTopKDirect-32      604461  1823 ns/op  2160 B/op 13 allocs/op
BenchmarkMZ037SQLIncrementalTopKDirect-32      665110  1806 ns/op  2160 B/op 13 allocs/op
BenchmarkMZ037SQLIncrementalTopKDirect-32      621457  1807 ns/op  2160 B/op 13 allocs/op
BenchmarkMZ037SQLIncrementalTopKDirect-32      643244  1855 ns/op  2160 B/op 13 allocs/op
BenchmarkMZ037SQLIncrementalTopKAdapter-32     447998  2583 ns/op  2832 B/op 17 allocs/op
BenchmarkMZ037SQLIncrementalTopKAdapter-32     444082  2710 ns/op  2832 B/op 17 allocs/op
BenchmarkMZ037SQLIncrementalTopKAdapter-32     432090  2783 ns/op  2832 B/op 17 allocs/op
BenchmarkMZ037SQLIncrementalTopKAdapter-32     442830  2695 ns/op  2832 B/op 17 allocs/op
BenchmarkMZ037SQLIncrementalTopKAdapter-32     431292  2584 ns/op  2832 B/op 17 allocs/op
```
