# CH-010: Materialized and Default Columns

`TypedTableColumn.Generated` already provided a write-time callback, but its
behavior was implicit: the callback always replaced the caller's value and
columns had to be evaluated in schema order. CH-010 makes the behavior
explicit while preserving that compatibility default.

## API

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
    Name: "orders",
    Columns: []hatSql.TypedTableColumn{
        {Name: "price", Kind: hatSql.TypedTableInt64},
        {Name: "quantity", Kind: hatSql.TypedTableInt64},
        {
            Name:                 "total",
            Kind:                 hatSql.TypedTableInt64,
            GeneratedMode:        hatSql.TypedTableGeneratedDefault,
            GeneratedDependencies: []string{"price", "quantity"},
            Generated: func(values []hatSql.TypedTableValue) (hatSql.TypedTableValue, error) {
                return hatSql.TypedInt64(values[0].Int64 * values[1].Int64), nil
            },
        },
    },
})
```

`TypedTableGeneratedMaterialized` is the zero value and the compatibility
mode. Its callback always runs and its result replaces the supplied value.
`TypedTableGeneratedDefault` runs the callback only when the candidate value is
null; a valid supplied value is retained and checked against the declared
column kind. A complete `Upsert` row has no omitted-value marker, so null is
the representation of a missing default.

`GeneratedDependencies` is optional metadata for generated columns. Names are
trimmed and validated at `NewTypedTable` time. Unknown, duplicate, empty, and
cyclic dependencies are rejected. Generated dependencies are evaluated in a
cached topological order; ordinary columns only document inputs and do not add
work to that order. The callbacks remain Go functions rather than parsed SQL
expressions, so callers retain responsibility for deterministic, side-effect
free computation.

Columnar append uses the same generated-column path. Existing generated-column
schemas and all callers that leave `GeneratedMode` at its zero value retain
their previous behavior.

## Allocation Fast Path

When every default column has an explicit value, the write path now avoids
cloning the candidate row because no callback can change it. It clones lazily
only when the first materialized callback or missing default must be applied.
This keeps the optimization local to the new default mode and does not change
callback isolation.

## Benchmark

Command:

```text
make benchmark-ch010-materialized-default
```

The benchmark performs 100,000 repeated upserts per sample, uses five samples,
and reports the median. It ran on an AMD Ryzen 9 5950X, Linux amd64. These are
single-threaded write-path measurements; they are not a claim about complete
SQL query performance.

| Path | Median ns/op | B/op | allocs/op | Relative to plain time | Relative heap | Relative allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain two-column upsert | 790.2 | 817 | 4 | 1.00x | 1.00x | 1.00x |
| Materialized generated column | 934.0 | 1,009 | 6 | 1.18x | 1.24x | 1.50x |
| Default column computed from null | 1,044 | 1,009 | 6 | 1.32x | 1.24x | 1.50x |
| Default column with explicit value | 874.0 | 817 | 4 | 1.11x | 1.00x | 1.00x |

The explicit-default fast path therefore eliminates the extra generated-row
allocation and returns to the plain path's measured heap and allocation count.
Computed defaults still pay for callback evaluation and row isolation. In the
before/after optimization check, explicit defaults moved from 914.7 ns/op,
913 B/op, and 5 allocations to 874.0 ns/op, 817 B/op, and 4 allocations; the
allocation and heap improvements are more stable than the timing difference.

## Verification

Focused correctness:

```text
make format-ch010-materialized-default
make test-ch010-materialized-default
```

The tests cover materialized compatibility, explicit and missing defaults,
dependency ordering, invalid explicit kinds, unknown dependencies, duplicate
dependencies, and cycles. Broader package, race, and vet checks are recorded in
`BENCHMARK.md` and the final change report.
