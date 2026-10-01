# T-U03 Stored Function Registry

This is the Tarantool-inspired stored-procedure registry gap. It adds an
importable, opt-in `hatAuth.StoredFunctionRegistry` for trusted in-process Go
handlers with stable names, version checks, function-level authorization,
bounded input/output, context propagation, and panic isolation.

## Contract

- A registry requires a non-nil `StoredFunctionAuthorizer`; a new registry
  fails closed instead of silently allowing calls.
- `Register` requires the `REGISTER` operation and a positive version. A new
  higher version atomically replaces the current handler. Equal or lower
  versions are rejected, preventing stale control-plane updates.
- `Call` requires the `CALL` operation. Version zero selects the current
  version; a nonzero version must match exactly, so a caller can fail closed
  during a rolling deployment.
- Function names and input/output sizes are bounded. Input and output bytes
  are copied at the registry boundary so handlers and callers cannot alias
  each other.
- Panics are recovered as the generic `ErrStoredFunctionPanic`; panic values
  are not returned to the caller. Handlers still run in-process and are
  trusted code, not a sandbox.
- A cancelled context is rejected before execution and checked again after
  the handler returns. Handlers should also observe the context themselves.
- `PolicyStoredFunctionAuthorizer` maps the existing object-aware RBAC policy
  to `function:<name>` objects and `REGISTER`/`CALL` commands.

T-U04 sandboxed Lua/runtime functions remain intentionally separate: this
feature does not attempt to make arbitrary code safe to execute.

## Example

```go
policy := hatAuth.Policy{
    Principals: map[string][]string{
        "admin": {"operator"},
        "alice": {"reader"},
    },
    Roles: []hatAuth.Role{
        {Name: "operator", Rules: []hatAuth.Rule{
            {Commands: []string{"REGISTER"}, Objects: []string{"function:*"}},
        }},
        {Name: "reader", Rules: []hatAuth.Rule{
            {Commands: []string{"CALL"}, Objects: []string{"function:echo"}},
        }},
    },
}
registry, err := hatAuth.NewStoredFunctionRegistry(hatAuth.StoredFunctionRegistryOptions{
    Authorize: hatAuth.PolicyStoredFunctionAuthorizer(policy),
})
if err != nil { /* handle invalid setup */ }

_, err = registry.Register("admin", hatAuth.StoredFunctionSpec{
    Name: "echo", Version: 1,
    Handler: func(ctx context.Context, input []byte) ([]byte, error) {
        return append([]byte("ok:"), input...), nil
    },
})
if err != nil { /* handle denied or invalid registration */ }

output, err := registry.Call(ctx, "alice", "echo", 1, []byte("payload"))
```

The registry does not persist handlers, distribute code, or change command
dispatch. Callers own registration lifecycle, durable version records, and
the trust boundary around the handler implementation.

## Measured Tradeoff

Five `-benchmem` samples ran on an AMD Ryzen 9 5950X:

| Path | Median | Bytes/op | Allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Direct owned handler call | 15.43 ns/op | 8 | 1 | baseline |
| Authorized registry call | 65.40 ns/op | 16 | 2 | 4.24x slower; 2x bytes; 2x allocations |

The extra cost is bounded and intentional: one authorization check, one
read-lock-protected version lookup, and input/output ownership copies. The
registry is opt-in, so ordinary cache and SQL paths pay no cost. If a caller
already has an isolated immutable input/output contract and does not need
authorization or version fencing, a direct handler remains the faster choice.
