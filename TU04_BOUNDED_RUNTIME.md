# T-U04 Bounded Runtime

## Status

Partially adopted as the importable `hatRuntime` package. It provides a small,
capability-free bytecode runtime for bounded stored-function style computation.
The existing SQL LuaJIT adapter remains build-tag gated and is not changed by
this primitive; SQL registry integration belongs to the SQL execution owner.

## Why This Shape

The useful ideas from ClickHouse, Materialize, and Tarantool are resource
budgets, deterministic execution boundaries, and a narrow function capability
surface. The implementation deliberately uses a closed instruction set instead
of embedding a general-purpose language. That makes instruction, stack, step,
argument, and string limits enforceable without exposing filesystem, network,
process, reflection, or host-callback capabilities.

This is suitable for expression-like stored functions and row-local computed
values. It is not a Lua compatibility layer and it is not a replacement for a
trusted Go callback.

## Example

```go
program, err := hatRuntime.Compile(1, []hatRuntime.Instruction{
	{Op: hatRuntime.OpLoadArg, Operand: 0},
	{Op: hatRuntime.OpPushInt, Operand: 2},
	{Op: hatRuntime.OpAddInt},
	{Op: hatRuntime.OpReturn},
})
if err != nil {
	return err
}

result, err := hatRuntime.Execute(ctx, program, []hatRuntime.Value{
	hatRuntime.Int64(40),
})
// result is Int64(42).
```

Use `CompileWithLimits` when a caller needs tighter budgets. `ExecuteBatch`
reuses one immutable program across isolated argument rows and stops on the
first row error.

## Default Budgets

| Resource | Default | Hard cap |
| --- | ---: | ---: |
| Arguments | 32 | 256 |
| Instructions | 4,096 | 65,536 |
| Stack values | 256 | 4,096 |
| String bytes | 64 KiB | 1 MiB |
| Execution steps | 1,000,000 | 100,000,000 |

The context is checked during execution, integer addition detects overflow,
and string concatenation is charged against the string-byte budget. Invalid
jumps, missing returns, invalid operands, type mismatches, stack overflow,
step exhaustion, and cancellation are explicit errors.

## Supported Operations

The initial set is intentionally small: null, boolean, signed 64-bit integer
and string constants; argument loading; integer addition; equality; string
concatenation; bounded jumps; conditional jumps; and return.

There is no dynamic dispatch, user callback, allocation hook, I/O operation,
map/object access, reflection, or unsafe escape from the value model.

## Verification

The focused tests cover arithmetic, invalid programs, loop step exhaustion,
stack limits, cancellation, string limits, and per-row batch isolation. The
package also passes the race detector and `go vet`.

## Benchmark

AMD Ryzen 9 5950X, Linux amd64, Go benchmark with five samples. Median values:

| Workload | Direct Go | Bounded runtime | Runtime / direct | Direct memory | Runtime memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Integer add | 0.51 ns/op, 0 allocs | 76.65 ns/op, 1 alloc | 150x | 0 B/op | 128 B/op |
| String concat | 22.15 ns/op, 1 alloc | 100.0 ns/op, 2 allocs | 4.5x | 4 B/op | 132 B/op |
| 256-row string batch | 7,243 ns/op, 257 allocs | 28,092 ns/op, 513 allocs | 3.9x | 10,496 B/op | 43,264 B/op |

The overhead is the cost of validation, bounded interpretation, and isolated
value handling. It is acceptable only where the safety boundary matters; hot
trusted expressions should remain direct Go or use a specialized compiled
path. No default SQL path is switched to this runtime by this change.
