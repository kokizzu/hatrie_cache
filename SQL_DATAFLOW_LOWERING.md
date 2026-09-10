# SQL Dataflow Lowering

`hatSql.CompiledSQLQuery.LowerDataflow` exposes a reusable, versioned logical
plan for callers that need to register, route, explain, or coordinate a SQL
query with an incremental dataflow system.

```go
compiled, err := hatSql.CompileSQLQuery(
    "SELECT name FROM CACHE('users') WHERE score >= $1 ORDER BY name LIMIT 2",
)
if err != nil {
    return err
}

plan := compiled.LowerDataflow()
// plan.Format == "hatrie-cache-sql-dataflow/v1"
// plan.Fragments == SCAN -> FILTER -> PROJECT -> SORT -> LIMIT
```

## Contract

- `Format` identifies the serialized plan contract.
- `Fragments` are deterministic and use stable zero-based IDs.
- `Inputs` point to earlier fragment IDs.
- `Root` identifies the last fragment, or `-1` for an empty plan.
- `Source` retains the original SQL source.
- Every call returns independent slices. Callers may retain or mutate their
  snapshot without changing the compiled query or another caller's snapshot.
- The private lowering is lazy and initialized once, so ordinary compiled-query
  users do not pay plan construction unless they call `LowerDataflow` or
  `Dataflow`.

The currently lowered stages are `SCAN`, `JOIN`, `FILTER`, `AGGREGATE`,
`HAVING`, `PROJECT`, `DISTINCT`, `SORT`, `LIMIT`, and `UNION` when present.

## Execution Boundary

The fragments are structural and reusable metadata. They do not replace the
existing SQL executor, automatically maintain differential results, or change
storage, wire protocols, query defaults, or backup formats. Callers that need
incremental execution can use the fragment IDs and details to select existing
typed or differential operators. Reusable executable composition is available
through `CompileSQLDataflow` or `CompiledSQLQuery.CompileDataflow`; the caller
still supplies the operator runner, so lowering does not change the existing
SQL executor.

## Performance

The plan is cached privately after its first request. On the benchmark query,
repeated `LowerDataflow` calls measured a median `182.3 ns/op`, `352 B/op`, and
`5 allocs/op`; the compatible `Dataflow` view measured `373.3 ns/op`, `704
B/op`, and `10 allocs/op`. A first request adds the one-time lowering work, so
callers that never inspect plans remain on the ordinary compile path. See
[`BENCHMARK.md`](BENCHMARK.md#sql-dataflow-lowering) for raw runs and the
controlled compile comparison.
