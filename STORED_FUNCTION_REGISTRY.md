# Stored Function Registry

`hatFunction.StoredFunctionRegistry` is an importable, opt-in registry for
trusted in-process functions. It gives callers stable names, versions, arity,
determinism metadata, bounded capacity, cancellation checks, and deterministic
introspection without silently executing arbitrary code from SQL or a wire
request.

## Usage

```go
registry, err := hatFunction.NewStoredFunctionRegistry(
	hatFunction.StoredFunctionRegistryOptions{MaxFunctions: 128, MaxArguments: 8},
)
err = registry.Register(hatFunction.StoredFunction{
	Name:          "math.increment",
	Version:       1,
	Arity:         1,
	Deterministic: true,
	Handler: func(ctx context.Context, args []any) (any, error) {
		return args[0].(int64) + 1, nil
	},
})
result, err := registry.Call(ctx, "math.increment", []any{int64(41)})
```

Names are trimmed, ASCII-normalized to lowercase, and restricted to letters,
digits, `_`, and `.`. Versions are positive metadata and duplicate normalized
names are rejected. `Arity` is exact when nonnegative and variadic when `-1`.
`List` returns handler-free metadata sorted by name; `Lookup` and
`RegisteredFunction` do not expose implementation closures.

## Safety Boundary

The registry is a trusted extension boundary, not a sandbox. A handler can
consume CPU, memory, or perform I/O, and authorization remains the caller's
responsibility. A context that is already canceled is rejected before handler
execution, but cancellation cannot forcibly stop a handler that ignores the
context. Handler panics become `ErrStoredFunctionPanic` and do not crash the
caller process.

The argument slice is copied before dispatch so a handler cannot reorder or
replace the caller's slice entries. Referenced values are not deep-copied.
Function count, name length, and argument count are bounded. A zero-value
registry is rejected rather than lazily allocating unbounded state.

## Integration

No SQL grammar, command route, replication path, or default configuration is
changed. A host application must explicitly construct the registry, authorize
the requested function, and decide how function versions participate in query
planning or persistence. This keeps code execution out of existing request
paths until a caller supplies that policy.

## Benchmark

Linux/amd64, AMD Ryzen 9 5950X, five repetitions. The fair baseline copies a
one-element `[]any` and invokes the same handler shape without registry lookup.
The registry benchmark uses one normalized name and the default call path.

| Path | Median | Memory | Relative cost |
| --- | ---: | ---: | ---: |
| Direct handler, inlined scalar | 0.2408 ns/op | 0 B/op, 0 allocs | reference only |
| Direct handler with owned args | 27.38 ns/op | 16 B/op, 1 alloc | reference |
| Registry call | 77.67 ns/op | 31 B/op, 2 allocs | 2.84x slower than fair baseline; +15 B and +1 alloc |

The registry pays this cost only when a caller chooses dynamic function
dispatch. Static hot paths should continue using direct functions.
