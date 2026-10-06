# T-U04 SQL Runtime Integration

`hatSql.NewRuntimeFunctionResolver` adapts immutable `hatRuntime.Program`
values to the vectorized `hatSql.FunctionResolver` contract.

```go
program, err := hatRuntime.Compile(1, []hatRuntime.Instruction{
	{Op: hatRuntime.OpLoadArg, Operand: 0},
	{Op: hatRuntime.OpPushInt, Operand: 2},
	{Op: hatRuntime.OpAddInt},
	{Op: hatRuntime.OpReturn},
})
functions, err := hatSql.NewRuntimeFunctionResolver(hatSql.RuntimeFunction{
	FunctionDefinition: hatSql.FunctionDefinition{
		Name:          "ADD_TWO",
		Deterministic: true,
		Pure:          true,
	},
	Program: program,
})
```

The resolver is opt-in and immutable. It can be embedded in a normal SQL
source resolver or combined with other function providers using the existing
`SQLFunctionResolverChain`. Unknown function names return
`ErrSQLFunctionNotHandled`, so a chain can continue to the next provider.

Only bounded scalar values are accepted: `NULL`, booleans, signed integers,
representable unsigned integers, strings, and byte slices. Floating-point and
compound values fail closed. Runtime type mismatches are reported as
`ErrRuntimeFunctionArgument`. The adapter executes a vectorized batch through
`hatRuntime.ExecuteBatch`; programs retain their instruction, stack, string,
and step limits. The function resolver interface has no request context, so
execution uses a background context and relies on the runtime step bound.

The adapter does not register functions globally, does not enable them by
default, and exposes no file, network, Go callback, or process access. The
`Deterministic` and `Pure` planner capabilities are only reported when the
caller explicitly declares them.

## Measurement

AMD Ryzen 9 5950X, Linux amd64, Go benchmark `-count=5`; the reported value is
the median sample.

| Workload | Direct bounded runtime | SQL runtime resolver | Change |
|---|---:|---:|---:|
| One integer call | 106.6 ns/op, 160 B/op, 2 allocs/op | 226.9 ns/op, 208 B/op, 4 allocs/op | 2.13x CPU, +30.0% bytes, +2 allocs |
| 256 integer calls/batch | 21,935 ns/op, 42,264 B/op, 258 allocs/op | 30,980 ns/op, 63,144 B/op, 263 allocs/op | 1.41x CPU, +49.4% bytes, +5 allocs |

The overhead is limited to callers that explicitly install the adapter. It is
accepted for the SQL integration because it adds a sandboxed function path
without changing ordinary SQL execution or introducing an unsafe callback
surface.
