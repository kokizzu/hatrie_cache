# MZ-034: Incremental SQL Projection Lowering

This feature adds an opt-in `CompiledSQLQuery.CompileIncrementalProjection`
adapter for exact signed updates through a bounded row-level SQL projection.
It is the projection counterpart to
`CompiledSQLQuery.CompileIncrementalFilter` and reuses the existing SQL scalar
expression evaluator, so expressions do not acquire a second semantics.

## Supported Shape

The adapter accepts one `CACHE` or `VALUES` source with:

- one or more explicit scalar `SELECT` expressions;
- unique output column names, using SQL aliases where appropriate;
- an optional scalar `WHERE` expression;
- no parameters, joins, grouping, aggregates, windows, ordering, limits,
  `DISTINCT`, CTEs, unions, custom functions, or other global semantics.

Unsupported shapes return `ErrSQLIncrementalProjectionUnsupported` instead of
silently changing query semantics. `SELECT * ... WHERE` remains handled by the
incremental filter adapter.

## Example

```go
compiled, err := hatSql.CompileSQLQuery(
    "FROM CACHE('items') SELECT id AS id, value + 1 AS next_value WHERE value >= 0",
)
if err != nil {
    return err
}
projection, err := compiled.CompileIncrementalProjection()
if err != nil {
    return err
}

changes, err := projection.Apply([]hatSql.DifferentialRow{
    {
        Key: "item-1", Time: 7, Diff: -3,
        Row: hatSql.Row{"id": "a", "value": int64(3)},
    },
})
// changes contains item-1 at time 7 with diff -3 and
// Row{"id": "a", "next_value": int64(4)}.
```

`Apply` is atomic: a malformed row or expression error returns no partial
output. Accepted updates preserve the caller's `Key`, `Time`, and signed
`Diff`; output rows contain only the selected columns and do not alias the
source row.

The adapter is opt-in and does not alter normal SQL execution or automatically
wire a planner to a changefeed. The caller remains responsible for supplying
the correct before/after signed row images and synchronizing concurrent calls.

## Measurement

Machine: AMD Ryzen 9 5950X, Go benchmark with `-benchmem -count=5`.

The baseline rebuilds the same projection over 10,000 source rows for every
single-row change. The incremental path evaluates one signed row.

| Path | Median time | Bytes/op | Allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: |
| Full SQL rebuild | 7,259,441 ns | 6,885,732 | 40,014 | baseline |
| Incremental projection | 555.7 ns | 384 | 3 | 13,063x faster; 17,932x less bytes; 13,338x fewer allocations |

Raw benchmark commands and samples are kept in the repository test files:

```text
make baseline-mz034-incremental-sql-projection
make benchmark-mz034-incremental-sql-projection
```

## Verification

```text
make format-mz034-incremental-sql-projection
make test-mz034-incremental-sql-projection
make test-mz034-incremental-sql-projection-package
make race-mz034-incremental-sql-projection
make vet-mz034-incremental-sql-projection
```
