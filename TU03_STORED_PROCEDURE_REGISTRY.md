# T-U03 Stored Procedure Registry

`hat/hatProcedure` provides a bounded, importable registry for trusted
in-process Go handlers. It is an explicit application API, not a scripting
runtime and not a network endpoint.

## Security Contract

- A registry cannot be created without an `Authorize` callback.
- Calls select an exact `(name, version)` pair; there is no implicit fallback.
- Procedure names are restricted to ASCII letters, digits, `_`, `-`, and `.`.
- Argument and result payloads have bounded defaults and are copied at the
  registry boundary.
- Handler panics become `ErrProcedurePanic` instead of unwinding the caller.
- The call context is passed to both authorization and handler execution.
- Registration is local and explicit. No procedures are loaded from disk,
  replication, or remote clients.

The handlers are trusted Go code. Sandboxed Lua or another untrusted runtime
remains a separate proposal and is deliberately not enabled by this package.

## Example

```go
registry, err := hatProcedure.NewRegistry(hatProcedure.RegistryOptions{
    Authorize: func(ctx context.Context, principal string, info hatProcedure.ProcedureInfo) error {
        if principal != "worker" || info.Name != "normalize" {
            return errors.New("denied")
        }
        return nil
    },
})
if err != nil {
    return err
}

err = registry.Register(hatProcedure.Definition{
    Name:    "normalize",
    Version: 1,
    Handler: func(ctx context.Context, call hatProcedure.Call) ([]byte, error) {
        return bytes.ToLower(call.Arguments), nil
    },
})
if err != nil {
    return err
}

result, err := registry.Call(ctx, "worker", "normalize", 1, input)
```

The package is imported directly as `hatrie_cache/hat/hatProcedure`; it is not
hidden behind an internal package.

## Defaults

The zero-valued limits are bounded to 1,024 registered versions, 128 name
bytes, and 1 MiB each for arguments and results. The registry is not created
automatically, so the default application path has no registry allocation or
call overhead.

## Measured Cost

The benchmark uses five `-count=5` samples on Linux/amd64 with an AMD Ryzen 9
5950X and a small 17-byte payload. The direct handler is a lower-bound
baseline, not an equivalent authorization or copying path.

| Operation | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Direct handler baseline | 0.2412 | 0 | 0 |
| Authorized registry call | 83.00 | 48 | 2 |
| Three-entry registry listing | 188.0 | 176 | 4 |

Raw samples are recorded in the T-U03 section of `BENCHMARK.md`. The measured
cost is intentional: authorization and immutable payload boundaries are not
free. Applications that already have a trusted, in-process function call and
do not need versioning or authorization should keep using the direct call.
