# Differential Row Batches

`hatSql.DifferentialRow` is an exported Materialize-style update primitive:
`Key` identifies the logical row, `Time` identifies its frontier, and `Diff`
is a signed multiplicity. `ConsolidateDifferentialRows` combines equal key/time
updates without dropping negative changes or duplicate multiplicity.

```go
updates, err := hatSql.ConsolidateDifferentialRows([]hatSql.DifferentialRow{
    {Key: "ada", Time: 7, Diff: 1, Row: hatSql.Row{"name": "Ada"}},
    {Key: "ada", Time: 7, Diff: -1, Row: hatSql.Row{"name": "Ada"}},
})
// updates is nil because the two updates cancel.
```

The function requires a non-empty caller-defined key, treats `Time` as part of
the identity, removes zero-sum entries, preserves first surviving key order,
does not mutate its input, and rejects signed count overflow. The first row
for a key/time supplies the output row value; later rows contribute only their
signed diff.

This is a reusable batch primitive, not a claim that every SQL operator is
incrementally maintained. Generic differential execution, joins, and
aggregates remain separate open inspiration items.

## Measurement

`make benchmark-sql-differential-rows` consolidates 1,024 updates into 32
logical key/time groups. Five samples on the repository benchmark host were
about `43-46 us/op`, `128,349-128,350 B/op`, and `38 allocs/op`.
