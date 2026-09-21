# MZ-016: Source Schema Evolution

`hatSql.NewSQLSchemaEvolutionPlan` is an opt-in compatibility adapter for a
live source schema and a consumer schema. It precomputes the name mapping once
and can then adapt rows from the current source version into the expected
version.

```go
plan, err := hatSql.NewSQLSchemaEvolutionPlan(
    currentColumns,
    expectedColumns,
    hatSql.SQLSchemaEvolutionOptions{
        AllowAddedColumns:  true,
        AllowDroppedColumns: true,
        CurrentVersion:     "events-v2",
        ExpectedVersion:    "events-v1",
    },
)
row, err := plan.AdaptRow(currentRow)
```

The default is strict. A caller must explicitly allow additive columns, and a
dropped expected column is only accepted when that expected column is nullable;
it is then emitted as `NULL`. Existing columns require the same physical type
and metadata. A nullable current column cannot become non-nullable. Each row is
validated against the current schema, and added source fields are omitted from
the adapted row. `AdaptRows` preserves input order and stops at the first
invalid row.

The plan is immutable after construction. Schema and change accessors return
detached copies. Bounds are 4,096 columns by default, 65,536 maximum, and 256
bytes per opaque version token. The feature does not change default SQL
execution; source connectors or callers opt into the adapter at their schema
boundary.

## Measurement

Environment: Linux amd64, AMD Ryzen 9 5950X. Five samples, `-benchmem`:

| Path | Median ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Reused compatibility plan | 300.2 | 336 | 2 | 6.85x faster |
| Rebuild plan for every row | 2,057 | 1,384 | 8 | 1.00x |

Plan construction is intentionally outside the reused path's measured loop.
The reuse path is about 4.12x lower heap and 4x lower allocation count. Both
paths allocate the adapted output row; the comparison measures the cost of
rebuilding schema maps and validation for every row.

Raw samples:

```text
BenchmarkMZ016SchemaEvolutionPlanReuse-32          298.7 ns/op  336 B/op 2 allocs/op
BenchmarkMZ016SchemaEvolutionPlanReuse-32          296.0 ns/op  336 B/op 2 allocs/op
BenchmarkMZ016SchemaEvolutionPlanReuse-32          300.5 ns/op  336 B/op 2 allocs/op
BenchmarkMZ016SchemaEvolutionPlanReuse-32          300.2 ns/op  336 B/op 2 allocs/op
BenchmarkMZ016SchemaEvolutionPlanReuse-32          302.1 ns/op  336 B/op 2 allocs/op
BenchmarkMZ016SchemaEvolutionRebuildPerRow-32     1969   ns/op 1384 B/op 8 allocs/op
BenchmarkMZ016SchemaEvolutionRebuildPerRow-32     2057   ns/op 1384 B/op 8 allocs/op
BenchmarkMZ016SchemaEvolutionRebuildPerRow-32     2097   ns/op 1384 B/op 8 allocs/op
BenchmarkMZ016SchemaEvolutionRebuildPerRow-32     1944   ns/op 1384 B/op 8 allocs/op
BenchmarkMZ016SchemaEvolutionRebuildPerRow-32     2125   ns/op 1384 B/op 8 allocs/op
```

## Verification

```text
make test-mz016-schema-evolution
make benchmark-mz016-schema-evolution
make race-mz016-schema-evolution
make vet-mz016-schema-evolution
```

The focused commands pass. A full `hatSql` package run still has three
pre-existing typed-table arrangement-checkpoint failures:
`TestTypedTableAggregateArrangementCheckpointRestoresGlobalAggregate`,
`TestTypedTableAggregateArrangementCheckpointIsDeterministic`, and
`TestTypedTableAggregateArrangementCheckpointRestoresWithoutReplay`.
