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
