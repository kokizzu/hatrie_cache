# M038i SQL Incremental DISTINCT

`CompiledSQLQuery.CompileIncrementalDistinct` provides opt-in exact signed
maintenance for a restricted `SELECT DISTINCT` query.

## Supported Shape

- One `CACHE(...)` or `VALUES(...)` row source.
- Explicit scalar `SELECT` expressions.
- Optional scalar `WHERE` without custom functions.
- `DISTINCT` with no `ORDER BY`, `LIMIT`, `OFFSET`, grouping, joins, CTEs,
  unions, windows, or other global stages.

Unsupported shapes return `ErrSQLIncrementalDistinctUnsupported` and remain on
the normal SQL executor path.

## Usage

```go
compiled, err := hatSql.CompileSQLQuery(
    "FROM CACHE('events') WHERE active = true SELECT DISTINCT bucket",
)
if err != nil {
    return err
}
distinct, err := compiled.CompileIncrementalDistinct()
if err != nil {
    return err
}

changes, err := distinct.Apply([]hatSql.DifferentialRow{
    {Key: "event-1", Time: 10, Diff: 1, Row: hatSql.Row{"bucket": int64(7), "active": true}},
})
```

Input keys identify source rows. The operator derives a deterministic typed
canonical key from the projected row, so duplicate source rows share one
visible output row while their positive multiplicities are retained internally.
The first insertion emits `Diff=+1`; retractions emit nothing until the final
duplicate leaves, which emits `Diff=-1`. `Snapshot` returns the current visible
set for deterministic replay.

Projection and canonicalization happen before state mutation. A malformed row,
unsupported value, negative multiplicity, or conflicting key returns an error
without partially applying the batch. The operator is single-writer and does
not enable automatic maintenance for existing materialized views.

## Measurement

Command: `make benchmark-m038-sql-distinct-baseline`

One `-benchmem` sample on Linux/amd64, AMD Ryzen 9 5950X, using 4,096 source
rows with 256 repeated projected values:

| Path | ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Rebuild `SELECT DISTINCT` over 4,096 rows | 295,263 | 270,043 | 541 | baseline |
| Warm incremental two-row insert/retract delta | 2,979 | 2,437 | 23 | 99.1x faster, 110.8x lower bytes, 23.5x fewer allocs |

The incremental path pays persistent state memory for the distinct set and
canonical row keys. The benchmark measures update latency, not initial state
hydration; callers should seed it from a consistent snapshot before consuming
changes. Broad M038 coverage for joins, grouping, set operations, and other
global SQL operators remains open.
