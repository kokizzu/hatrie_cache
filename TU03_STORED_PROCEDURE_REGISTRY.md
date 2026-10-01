# Tarantool-Style Stored Procedure Registry

`hatSql.SQLProcedureRegistry` is an importable, opt-in registry for named
read-only SQL procedures. A procedure is compiled once and then called by name
with positional `$1`, `$2`, ... values. The registry is caller-owned; no server,
HTTP/2, gRPC, journal, or storage path constructs one automatically.

## Defaults and limits

`NewSQLProcedureRegistry(SQLProcedureRegistryOptions{})` allows up to 256
procedures. The caller can set `MaxProcedures` from 1 through 4096. The feature
is disabled by default because callers must explicitly create and retain the
registry. Registration rejects non-query sources, duplicate names, invalid
parameter metadata, and sources that fail SQL compilation.

The registry only accepts read-only query sources beginning with `FROM`,
`SELECT`, or `WITH`. It does not provide authorization, durable persistence,
Lua or other scripting, panic isolation, version migration, or mutation
execution. Callers must authorize definitions and any custom SQL functions
before registration.

## Example

```go
registry, err := hatSql.NewSQLProcedureRegistry(hatSql.SQLProcedureRegistryOptions{})
if err != nil {
	return err
}
err = registry.Register(hatSql.SQLProcedureDefinition{
	Name: "user_by_id",
	Source: "FROM VALUES (1, 'Ada'), (2, 'Lin') AS users(id, name) " +
		"WHERE users.id = $1 SELECT users.name",
	Parameters: []string{"id"},
})
if err != nil {
	return err
}

result, err := registry.Call(
	context.Background(),
	"USER_BY_ID",
	nil,
	[]interface{}{int64(2)},
	hatSql.SQLQueryOptions{},
)
if err != nil {
	return err
}
// result.Columns == []string{"name"}
// result.Rows == []hatSql.SQLRow{{"name": "Lin"}}
```

Procedure names are case-insensitive and trimmed. `Definition` and
`Definitions` return copies of the metadata, with `Definitions` sorted by
name. `Call` checks the supplied value count against the declared parameter
metadata before invoking the compiled query. Compiled query execution remains
safe for concurrent calls; registration and metadata reads use a small
registry lock.

## Tradeoffs

- Repeated calls avoid parser and prepared-cache lookup work.
- The benchmarked call path is 2.43x faster, with 2.30x lower per-call
  allocation bytes and 2.11x fewer allocations than compiling every call.
- The compiled plan and metadata remain resident until the registry is
  discarded. The bounded registry limit prevents unbounded growth, but there
  is no eviction or unregister operation.
- Parameter binding and query execution still allocate; this is a plan-reuse
  optimization, not a zero-allocation result path.
- The benchmark excludes one-time registration/compilation, resolver latency,
  authorization, and network or storage costs.

See [BENCHMARK.md#t-u03-read-only-sql-procedure-registry](BENCHMARK.md#t-u03-read-only-sql-procedure-registry)
for raw samples and the reproducible command.
