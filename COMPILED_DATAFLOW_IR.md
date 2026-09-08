# Compiled Dataflow IR

`hatSql.CompileSQLQuery` now exposes a stable logical plan snapshot through
`CompiledSQLQuery.Dataflow`:

```go
compiled, err := hatSql.CompileSQLQuery(
	"SELECT name FROM CACHE('users') WHERE score >= $1 ORDER BY name LIMIT 2",
)
if err != nil {
	return err
}
ir := compiled.Dataflow()
```

`SQLDataflowIR.Nodes` contains deterministic logical stages such as `SCAN`,
`FILTER`, `PROJECT`, `SORT`, and `LIMIT`. Each node has an ID and input IDs;
`Root` identifies the final stage. `Source` retains the original SQL template.
Every call returns fresh slices, so routing, admission, explain, and plan
registry code can inspect or annotate a snapshot without mutating the compiled
query.

This is an inspection and coordination boundary, not a new executor. Compiled
query execution still clones and runs the existing template, preserving result
ordering, parameter binding, error behavior, and all existing optimization
fallbacks. It adds no storage, wire, or configuration format. Lowering nodes
into independently executable reusable fragments remains the separate `M052`
goal.

The five-run local benchmark used the command below:

```text
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCompiledSQLQueryDataflow$' -benchmem -benchtime=200ms -count=5
```

The median snapshot cost was `728.9 ns/op`, `1,097 B/op`, and `13 allocs/op`.
Call `Dataflow` only when the metadata is needed; ordinary compiled execution
does not call it.
