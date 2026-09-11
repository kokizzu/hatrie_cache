# SQL Dataflow Fragment Executor

`CompileSQLDataflow` provides a small execution boundary for a lowered
`SQLDataflowPlan`. It validates and snapshots the plan, then invokes a caller
owned runner once for each fragment in ID order. The runner supplies the
operator semantics, so this API can be used by typed, incremental, or
differential query components without changing the existing SQL executor.

## Basic Usage

The usual entry point is a compiled query. Compilation happens once; each
`Execute` call supplies a new input snapshot:

```go
compiled, err := hatSql.CompileSQLQuery(
    "FROM VALUES (1) AS src(id) SELECT src.id",
)
if err != nil {
    return err
}

executor, err := compiled.CompileDataflow(func(
    ctx context.Context,
    fragment hatSql.SQLDataflowFragment,
    inputs hatSql.SQLDataflowFragmentInputs,
) ([]hatSql.SQLRow, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    rows := inputs.Initial()
    if inputs.Len() > 0 {
        rows = inputs.Rows(0)
    }
    // Apply the operator represented by fragment.Kind here.
    return rows, nil
})
if err != nil {
    return err
}

rows, err := executor.Execute(ctx, []hatSql.SQLRow{{"id": int64(1)}})
```

The runnable external-package example is in
`hat/hatSql/m052b_dataflow_fragment_example_test.go`.

## Contract

- `CompileDataflow` lowers a `CompiledSQLQuery`, validates the plan, and
  retains an independent snapshot. `CompileSQLDataflow` accepts a plan from
  `LowerDataflow` when a caller needs to compose or transform it first.
- Fragment IDs must be contiguous and match their positions. Every input must
  refer to an earlier fragment, which rejects cycles and forward references.
- `SQLDataflowFragmentInputs.Initial` returns the input passed to `Execute`.
  `Len`, `FragmentID`, and `Rows` expose the already-produced upstream outputs
  without constructing an input slice for every callback.
- The runner must treat the fragment, input rows, and input metadata as
  read-only during the callback. It owns the returned row slice. An executor
  can be reused, including concurrently, when the runner itself is safe for
  concurrent calls.
- A canceled context is rejected before execution, checked before each
  fragment, and checked after each callback. Runner errors are wrapped with
  the fragment ID and kind while preserving `errors.Is` matching.
- The API does not silently execute SQL expressions. The runner must implement
  the semantics it needs. Existing `Execute` and `ExecuteRows` behavior,
  defaults, storage, wire formats, and backup formats are unchanged.

An empty validated plan returns no rows and no error. Nil runners, malformed
plans, nil contexts, and nil executors return explicit errors.

## Built-In Native Batch Path

`CompiledSQLQuery.CompileNativeDataflow` is an opt-in built-in executor for a
small, allocation-conscious dataflow shape. It accepts one already-resolved
`CACHE` or `KEYS` batch and fuses the scalar `WHERE` predicate with scalar
`SELECT` projection:

```go
compiled, err := hatSql.CompileSQLQuery(
    "FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 2048",
)
if err != nil {
    return err
}

executor, err := compiled.CompileNativeDataflow()
if err != nil {
    return err
}

rows, err := executor.Execute(ctx, inputRows)
```

The native handle is reusable. It checks the context before each input row,
validates typed source fields, preserves row order, duplicates, and SQL
`NULL` filtering semantics, and does not mutate the input rows. Scalar
evaluation errors retain the normal SQL behavior. `ErrSQLNativeDataflowUnsupported`
is returned at compilation time for joins, unions, CTEs, mixed aggregate and
non-aggregate projections, grouping, window functions, ordering, limits,
samples, `DISTINCT`, `*`, custom function calls, and other shapes outside the
supported built-in paths. Callers use the ordinary `CompiledSQLQuery.Execute`
or callback-backed `CompileDataflow` path for those queries.

Aggregate-only single-source queries are also supported when every selected
expression is one of `COUNT`, `SUM`, `AVG`, `MIN`, or `MAX`, with an optional
scalar `WHERE` predicate and no `GROUP BY`, `ORDER BY`, `LIMIT`, `OFFSET`,
`DISTINCT`, aggregate `FILTER`, window, join, or custom function. The native
executor returns one row, including an empty-input result with SQL-compatible
`COUNT` and `NULL` results for aggregates without values. It evaluates the
same typed source and SQL null semantics as the ordinary global aggregate
executor.

This API does not resolve sources, change query defaults, or replace the
ordinary SQL executor. It is a lower-level boundary for callers that already
own a consistent source batch; the source-resolution and snapshot contract
remain the caller's responsibility.

## Measurement

Command:

```text
make benchmark-m052b-dataflow-fragments
```

The benchmark transforms 128 map rows through five identical stages. Plan
compilation is outside the timed loop. The direct path and the executor path
run the same row transformation. Results below are the medians of five runs
on Linux/amd64 with an AMD Ryzen 9 5950X:

| Path | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Direct five-stage loop | 144,089 | 220,841 | 1,290 |
| Reusable fragment executor | 155,126 | 220,969 | 1,291 |

Raw timed runs were `143254, 144089, 141378, 144316, 145553 ns/op` for
direct and `145037, 147087, 155126, 157046, 156084 ns/op` for the executor.
The executor uses `1.077x` the paired direct CPU median, with `+128 B/op` and
`+1 alloc/op` for the per-execution dependency-output table. This is an
opt-in extensibility cost, not a claim that the callback boundary is faster
than an inlined loop. A direct-only pre-implementation run measured
`141098 ns/op`, `220841 B/op`, and `1290 allocs/op`; the paired control is used
for the comparison because separate benchmark invocations showed normal CPU
noise.

### Native Batch Measurement

Command:

```text
make benchmark-m052c-native-dataflow
```

The paired benchmark compiles the same single-source query, evaluates it over
4,096 map rows, and runs five samples for the ordinary compiled executor and
the opt-in native batch executor. Compilation is outside the timed loop. The
native path receives the same resolved rows directly, so the comparison
measures query execution overhead rather than source storage or wire transfer.

| Path | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Ordinary compiled executor | 1,673,810 | 2,657,606 | 12,304 | baseline |
| Native scalar batch executor | 899,440 | 720,928 | 4,098 | 1.86x faster; 3.69x fewer bytes; 3.00x fewer allocations |

Raw paired samples:

| Run | Ordinary ns/op | Native ns/op | Ordinary B/op | Native B/op | Ordinary allocs/op | Native allocs/op |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,673,810 | 904,438 | 2,657,605 | 720,928 | 12,304 | 4,098 |
| 2 | 1,667,308 | 883,108 | 2,657,613 | 720,928 | 12,304 | 4,098 |
| 3 | 1,659,304 | 883,223 | 2,657,615 | 720,928 | 12,304 | 4,098 |
| 4 | 1,688,848 | 906,509 | 2,657,606 | 720,928 | 12,304 | 4,098 |
| 5 | 1,687,189 | 899,440 | 2,657,606 | 720,928 | 12,304 | 4,098 |

The gain is conditional on the caller already having a source batch and on
the query fitting the supported scalar shape. Unsupported plans fail closed
instead of silently taking a partial semantic path, and the ordinary executor
remains the compatibility fallback.

### Native Global Aggregate Measurement

Command:

```text
make benchmark-m052d-native-aggregate
```

The paired benchmark evaluates five global aggregates over 4,096 already
resolved rows, with a scalar `WHERE` predicate. Compilation is outside the
timed loop. The ordinary compiled executor and native aggregate executor use
the same rows and five paired samples.

| Path | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Ordinary compiled executor | 1,920,319 | 2,996,500 | 16,475 | baseline |
| Native global aggregate executor | 1,122,827 | 1,379,473 | 12,317 | 1.71x faster; 2.17x fewer bytes; 1.34x fewer allocations |

Raw paired samples:

| Run | Ordinary ns/op | Native ns/op | Ordinary B/op | Native B/op | Ordinary allocs/op | Native allocs/op |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,829,136 | 1,122,827 | 2,996,837 | 1,379,473 | 16,478 | 12,317 |
| 2 | 1,944,206 | 1,087,612 | 2,996,500 | 1,379,444 | 16,475 | 12,317 |
| 3 | 1,805,740 | 1,102,521 | 2,996,626 | 1,379,474 | 16,476 | 12,317 |
| 4 | 1,920,319 | 1,130,396 | 2,996,483 | 1,379,449 | 16,475 | 12,317 |
| 5 | 1,930,984 | 1,151,369 | 2,996,477 | 1,379,498 | 16,475 | 12,317 |

The pre-implementation ordinary-only control measured 2,052,401 ns/op,
2,996,887 B/op, and 16,479 allocations/op across five samples. The paired
control is used for the reported ratio because it runs beside the native path
and avoids treating separate benchmark invocations as a code-path change.
The optimization is limited to the aggregate-only shape; ordinary SQL
execution remains the compatibility path for richer queries.
