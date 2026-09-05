# RowBinary Statistics Pruning

`CanSkipSQLRowBinaryStats` uses existing per-column min/max and NULL counts to
prove that a RowBinary block cannot satisfy a simple predicate. A `true`
result means the block is safe to skip; `false` never claims that a match
exists.

Supported operators are equality, inequality, `<`, `<=`, `>`, `>=`, inclusive
`BETWEEN`, `IS NULL`, and `IS NOT NULL`. Boundaries are conservative: for
example, a block whose minimum equals `x` cannot be skipped for `value <= x`.
All-null blocks can be skipped for ordinary comparisons and `IS NOT NULL`.
JSON and other non-orderable columns return an error for ordered predicates.

```go
stats, err := hatSql.BuildSQLRowBinaryColumnStats(columns, rows)
if err != nil {
    return err
}
skip, err := hatSql.CanSkipSQLRowBinaryStats(columns, stats,
    hatSql.SQLRowBinaryStatsPredicate{
        Column: "id",
        Operator: hatSql.SQLRowBinaryStatsGreater,
        Value: int64(1000),
    })
```

The API validates schema/statistics alignment and does not modify either
input. It is additive and does not automatically change query planning or
decoding behavior.

## Measurement

`make benchmark-sql-row-binary-stats-pruning` reports about `25.9-26.4 ns/op`,
`8 B/op`, and `1 alloc/op` for one integer equality check against a 1,024-row
statistics block on the repository benchmark host.
