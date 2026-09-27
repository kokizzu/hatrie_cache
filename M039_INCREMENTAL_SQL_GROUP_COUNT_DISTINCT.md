# M039 Incremental SQL `COUNT(DISTINCT)`

This feature lowers one exact grouped SQL shape into retained signed
differential state. It is importable and explicit; ordinary SQL execution
keeps its existing behavior.

## Supported Shape

The query must have:

- one `CACHE(...)` or `VALUES` source;
- one direct `GROUP BY` field that is projected;
- one `COUNT(DISTINCT direct_field)` projection;
- an optional scalar `WHERE` expression.

The distinct value must be a non-null `int64`. Values with a zero or negative
current multiplicity are not counted. Arbitrary expressions, joins, CTEs,
unions, parameters, window functions, ordering, limits, `HAVING`, grouping
sets, and multiple aggregates are rejected explicitly with
`ErrSQLIncrementalGroupCountDistinctUnsupported`.

## API

```go
compiled, err := hatSql.CompileSQLQuery(
    "FROM CACHE('items') AS src " +
        "SELECT src.group, COUNT(DISTINCT src.value) AS unique_values " +
        "GROUP BY src.group",
)
if err != nil {
    return err
}
operator, err := compiled.CompileIncrementalGroupCountDistinct()
if err != nil {
    return err
}

changes, err := operator.Apply([]hatSql.DifferentialRow{
    {Key: "row-1", Diff: 1, Row: hatSql.Row{"group": "red", "value": int64(5)}},
    {Key: "row-2", Diff: 1, Row: hatSql.Row{"group": "red", "value": int64(5)}},
    {Key: "row-3", Diff: 1, Row: hatSql.Row{"group": "red", "value": int64(7)}},
})
```

The first and second rows share one distinct value, so the resulting group
contains `unique_values = 2`. Retracting only one duplicate produces no
visible change. Retracting the second duplicate emits the exact transition
from `2` to `1`. Invalid rows fail the whole batch without partially applying
earlier rows. `Snapshot()` returns deterministic sorted group keys.

The materialized SQL parser and evaluator also accept standard
`COUNT(DISTINCT expression)` syntax. Automatic native grouped execution
rejects this form instead of treating it as ordinary `COUNT(value)`, so public
queries use the exact materialized path until a native distinct state is
implemented.

## Benchmark

Command:

```text
make baseline-m039-incremental-sql-group-count-distinct
```

The benchmark seeds the incremental operator with 10,000 rows, then times one
duplicate `int64` update. The rebuild path reconstructs the same 10,000-row
state for every operation. Five `-benchmem` samples were collected on
Linux/amd64 with an AMD Ryzen 9 5950X.

| Path | Median CPU | Allocated bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Rebuild from 10,000 differential rows | 5.577 ms | 7,965,703 B | 40,919 |
| Incremental one-row update | 434.6 ns | 80 B | 3 |
| Improvement | 12,832x faster | 99,571x lower | 13,640x lower |

Raw CPU samples, in command order:

- rebuild: `4.936388 ms`, `5.576928 ms`, `5.591167 ms`, `5.950085 ms`, `5.383521 ms`;
- incremental: `453.9 ns`, `434.6 ns`, `401.3 ns`, `421.6 ns`, `456.1 ns`.

Allocated bytes are transient per-operation allocations, not the retained
memory of the seeded operator. The incremental operator retains its exact
group/value multiplicity state, while the rebuild measurement constructs and
discards that state each time. The benchmark therefore measures update cost,
not a claim that retained state is free.

## Verification

The focused test covers materialized results, duplicate multiplicity,
retractions, `WHERE`, atomic rejection of invalid values, validation errors,
and deterministic snapshots:

```text
make test-m039-incremental-sql-group-count-distinct
```
