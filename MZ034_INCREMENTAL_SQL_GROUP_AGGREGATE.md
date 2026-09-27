# MZ034 Incremental SQL Group Aggregate

This is the Materialize-style incremental maintenance slice for bounded SQL
`GROUP BY` queries. It lowers a compiled query through
`CompiledSQLQuery.CompileIncrementalGroupAggregate` without changing the
normal SQL executor or enabling the path automatically.

## Supported Shape

The opt-in lowering accepts:

- one `CACHE` or `VALUES` source;
- one direct `GROUP BY` field, which must also be selected;
- `COUNT(*)`, `SUM(field)`, or both;
- an optional scalar `WHERE` expression without custom functions.

`SUM` uses the existing exact signed `int64` differential state and is exposed
as the normal SQL `float64` result type. `SUM` values must be non-null integer
values; nullable, floating-point, and other aggregate inputs return an error
instead of silently changing SQL semantics.

Joins, CTEs, unions, grouping sets, `HAVING`, `QUALIFY`, ordering, limits,
windows, `DISTINCT`, parameters, and other global semantics remain on the
ordinary executor and are rejected by the opt-in compiler.

## Differential Semantics

`Apply` accepts signed `DifferentialRow` updates. A changed group emits a
retraction of its previous aggregate row followed by an insertion of its new
row. Negative counts, integer overflow, missing source rows, and invalid SUM
values are rejected atomically. `Snapshot` returns one positive row per active
group in deterministic group-key order.

Example:

```go
compiled, _ := hatSql.CompileSQLQuery(
    "FROM CACHE('items') AS src " +
        "SELECT src.group AS bucket, COUNT(*) AS total, " +
        "SUM(src.score) AS total_score GROUP BY src.group",
)
operator, _ := compiled.CompileIncrementalGroupAggregate()
changes, _ := operator.Apply([]hatSql.DifferentialRow{
    {Key: "row-1", Diff: 1, Row: hatSql.Row{"group": "red", "score": int64(3)}},
})
// changes[0].Row == {"bucket": "red", "total": int64(1), "total_score": float64(3)}
```

## Benchmark

Command:

```text
make benchmark-mz034-incremental-sql-group
```

The benchmark uses 10,000 source rows across 32 groups. The rebuild case
executes the compiled SQL query over all rows. The incremental case seeds the
operator once and measures one subsequent signed update, so the numbers show
refresh cost versus update cost rather than equivalent total work.

Environment: AMD Ryzen 9 5950X, Go benchmark with `-benchmem`, five samples.

| Path | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rebuild time | 2.091 ms | 2.059 ms | 2.120 ms | 2.048 ms | 2.072 ms | 2.072 ms |
| Rebuild bytes | 1,898,649 | 1,897,800 | 1,898,400 | 1,898,179 | 1,897,876 | 1,898,179 |
| Rebuild allocs | 10,301 | 10,292 | 10,298 | 10,296 | 10,293 | 10,296 |
| Incremental time | 1,561 ns | 1,497 ns | 1,486 ns | 1,535 ns | 1,512 ns | 1,512 ns |
| Incremental bytes | 1,539 | 1,539 | 1,539 | 1,539 | 1,539 | 1,539 |
| Incremental allocs | 18 | 18 | 18 | 18 | 18 | 18 |

Relative to rebuilding the full source, one incremental update is approximately
`1,370x` lower latency, `1,233x` lower per-operation bytes, and `572x` fewer
allocations. The tradeoff is retained grouped state and the deliberately
restricted non-null integer `SUM` contract; the operator is opt-in and does
not yet perform automatic planner wiring.
