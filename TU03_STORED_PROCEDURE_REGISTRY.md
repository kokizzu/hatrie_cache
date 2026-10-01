# T-U03 Stored Procedure Registry

Status: partial, importable in-process registry.

`hatProcedure.Registry` provides a bounded versioned procedure table with
byte-oriented calls. It is inspired by Tarantool's stored function boundary,
but it does not add a scripting runtime or expose a network endpoint.

## Safety Contract

- A nil authorizer denies calls. `AllowAll` is explicit for callers that have
  already secured the registry boundary.
- Procedure names are bounded ASCII identifiers using letters, digits, `.`,
  `_`, and `-`, with an alphanumeric first and last character.
- Definitions are immutable by name/version. Register a new version instead
  of replacing a live handler.
- Version `0` in a call selects the newest registered version.
- Registry, version, payload, and result limits are bounded by options and
  validated at construction.
- Authorization and handlers run without the registry lock.
- Authorizer and handler panics become fixed errors; recovered values are not
  returned to callers.
- Caller payloads, authorizer payloads, handler payloads, and returned results
  are isolated byte slices. Handlers and authorizers must not retain them.

Example:

```go
registry, err := hatProcedure.NewRegistry(hatProcedure.Options{
	Authorize: authorizeProcedure,
})
if err != nil {
	return err
}
if err := registry.Register(hatProcedure.Definition{
	Name:    "cache.invalidate",
	Version: 1,
	Handler: invalidateCache,
}); err != nil {
	return err
}
result, err := registry.Invoke(ctx, hatProcedure.Call{
	Name:    "cache.invalidate",
	Payload: payload,
})
```

The registry remains caller-owned: callers must choose authorization policy,
durable registration, transport exposure, and procedure payload schemas.

## Benchmark

Machine: AMD Ryzen 9 5950X, linux/amd64. Five `-count=5` samples from
`make benchmark-round35-procedure`.

| Path | Median time | Memory | Relative time |
| --- | ---: | ---: | ---: |
| Direct handler call baseline | 0.483 ns/op | 0 B, 0 allocs | 1.00x |
| Authorized registry invoke | 120.4 ns/op | 24 B, 3 allocs | 249.2x slower |

The registry is therefore for controlled command/procedure boundaries, not
for per-row or per-key inner loops. The copies and panic/authorization guards
buy isolation and security at a measurable dispatch cost.

## Verification

```text
make format-round35-procedure
make test-round35-procedure
make benchmark-round35-procedure
make race-round35-procedure
make vet-round35-procedure
```
