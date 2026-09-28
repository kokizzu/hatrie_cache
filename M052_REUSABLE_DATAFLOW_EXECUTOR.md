# Reusable Dataflow Executor

Materialize-style dataflow plans are useful beyond one execution. The compiled
SQL query already memoizes its immutable fragment plan, but the original
`CompileDataflow` API copied every fragment and input slice when binding a
runner. `CompileReusableDataflow` reuses that memoized backing storage for
repeated executor construction.

```go
executor, err := query.CompileReusableDataflow(runner)
if err != nil {
	return err
}
rows, err := executor.Execute(ctx, initial)
```

The runner must treat `SQLDataflowFragment` metadata as read-only. Use
`CompileDataflow` when the caller needs an independent plan copy. The shared
path does not change SQL execution defaults and does not share execution output
buffers between calls.

The benchmark covers repeated executor construction over the same compiled
query. Results are recorded in [BENCHMARK.md](BENCHMARK.md#m052-shared-dataflow-executor).
