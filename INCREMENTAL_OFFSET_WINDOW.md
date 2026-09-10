# Incremental LAG and LEAD Windows

`hatSql.IncrementalOffsetWindow` is an append-only Materialize-style window
maintainer for `LAG` and `LEAD`. It emits ordinary `DifferentialRow` values so
an ordered stream can update a downstream arrangement without rescanning the
whole relation.

## Example

```go
window, err := hatSql.NewIncrementalOffsetWindow(hatSql.IncrementalOffsetWindowDefinition{
    Direction:    hatSql.IncrementalWindowLag,
    OutputColumn: "previous_score",
    PartitionKey: func(row hatSql.Row) (string, error) { return row["team"].(string), nil },
    OrderKey:     func(row hatSql.Row) (interface{}, error) { return row["sequence"], nil },
    RowKey:       func(row hatSql.Row) (string, error) { return row["id"].(string), nil },
    ValueKey:     func(row hatSql.Row) (interface{}, error) { return row["score"], nil },
    Offset:       1,
    DefaultValue: nil,
})
updates, err := window.Append([]hatSql.Row{
    {"id": "a", "team": "core", "sequence": int64(1), "score": int64(10)},
    {"id": "b", "team": "core", "sequence": int64(2), "score": int64(20)},
})
```

The output values are `NULL` for `a` and `10` for `b`. A `LEAD` window uses
`Direction: hatSql.IncrementalWindowLead`. Its trailing rows initially receive
`DefaultValue`; when a later row arrives, the maintainer emits a negative row
for the old default and a positive row for the newly known value. Rows added in
one batch use final same-batch lookahead directly, so transient default rows
are not returned.

## Contract

- `Offset` must be non-negative. Offset zero returns the current row value.
- `OrderKey` and `RowKey` are required. Rows must be non-decreasing within a
  partition, or non-increasing when `Descending` is true.
- `ValueKey` returns the value placed in the derived output column. Returning
  `nil` represents SQL `NULL`.
- Row identities must be non-empty and unique for the lifetime of the
  maintainer. The input row map is never modified.
- The complete batch is validated before state is published. Callback errors,
  duplicate identities, output-column collisions, and order violations leave
  the window unchanged.
- `LAG` retains only the last `Offset` values per partition. `LEAD` retains
  only unresolved tail rows, bounded by `Offset`; it does not retain the full
  input relation.
- The primitive is append-only and does not change ordinary SQL execution or
  defaults. Arbitrary row updates and deletes require a retained mutable
  arrangement and remain outside this feature.

## Benchmark

Command: `make benchmark-m065c-incremental-offset-window`.

The workload compares a full ordered recomputation of 1,025 rows with one
incremental tail append after a 1,024-row seed, using one row per operation in
16 partitions. Five samples were run on Linux/amd64 with an AMD Ryzen 9 5950X;
the benchmark uses a 200 ms sample window. The incremental seed and identity
map capacity are outside the timer.

| Window | Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| LAG | Full ordered scan | 407,016; 386,071; 390,370; 415,973; 387,629 | 390,370 | 526,873 | 2,219 | 1x |
| LAG | Incremental append | 928.1; 910.1; 907.4; 890.7; 958.6 | 910.1 | 704 | 10 | 429x faster |
| LEAD | Full ordered scan | 452,987; 425,797; 415,349; 448,276; 427,194 | 427,194 | 526,873 | 2,219 | 1x |
| LEAD | Incremental append | 1,832; 1,879; 1,869; 1,881; 1,966 | 1,879 | 2,143 | 18 | 227x faster |

For this workload, incremental `LAG` uses about `748x` fewer transient bytes
and `222x` fewer allocations. Incremental `LEAD` uses about `246x` fewer
transient bytes and `123x` fewer allocations. The baseline materializes every
row and sorts every partition; the incremental path is intended for ordered
append streams and does not claim the same result for arbitrary reordering.

## Verification

```text
make test-m065c-incremental-offset-window
make test-race-m065c-incremental-offset-window
make vet-m065c-incremental-offset-window
make benchmark-m065c-incremental-offset-window
```
