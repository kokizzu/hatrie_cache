# T-U03 Stored Procedure Registry

Hatrie SQL now has an opt-in `hatSql.StoredProcedureRegistry` for trusted
in-process stored procedures. It defines a stable versioned call boundary
without changing the existing `FunctionResolver` or creating a global
registry.

## Contract

Register an immutable `(name, version)` pair:

```go
registry := hatSql.NewStoredProcedureRegistry(hatSql.StoredProcedureRegistryOptions{
	Authorize: func(ctx context.Context, request hatSql.StoredProcedureRequest) error {
		return authorizeLocalProcedure(ctx, request.Name, request.Version)
	},
})

err := registry.Register(hatSql.StoredProcedureDefinition{
	Name:    "orders.total",
	Version: "v1",
	Execute: func(ctx context.Context, request hatSql.StoredProcedureRequest) (hatSql.StoredProcedureResponse, error) {
		return hatSql.StoredProcedureResponse{Values: []interface{}{int64(42)}}, nil
	},
})

values, err := registry.Call(ctx, "orders.total", "v1", nil)
```

Names are case-insensitive and trimmed. Versions are trimmed and exact-match;
publishing a new version is the replacement mechanism. Duplicate versions are
rejected, and `Versions(name)` returns deterministic sorted metadata.

Each call has bounded defaults of 64 arguments and 64 result values. They can
be lowered or raised per registry with `MaxArguments` and `MaxResults`; zero
or negative values select the defaults. Caller argument slices are copied
before authorization and execution. Result slices are transferred to the
caller and are never retained by the registry.

## Safety

- The registry is explicitly constructed and is not wired into SQL or network
  endpoints automatically.
- A nil authorizer is suitable only for a trusted local caller. Network-facing
  adapters should provide `StoredProcedureAuthorizer` before forwarding calls.
- Authorization runs before execution and receives a separate request copy.
- A nil context becomes `context.Background`; canceled contexts are rejected
  before execution and checked again after authorization.
- Procedure and authorizer panics are recovered as
  `ErrStoredProcedurePanic`; panic payloads are not copied into the error.
- Callbacks execute without the registry lock, so a slow procedure does not
  block registration or unrelated calls.
- The registry does not provide a sandbox. Untrusted scripting belongs to the
  separate T-U04 sandbox proposal.

## Measured Cost

Machine: AMD Ryzen 9 5950X, Linux/amd64. Five `-benchmem` samples from
`make benchmark-tu03`; medians are reported.

| Path | Median ns/op | B/op | Allocs/op | Relative to direct callback |
| --- | ---: | ---: | ---: | --- |
| Direct callback | 25.2 | 16 | 1 | baseline |
| Registry call, no authorizer | 114.8 | 32 | 2 | 4.56x time, 2.00x bytes, +1 alloc |
| Registry call, authorizer | 162.5 | 48 | 3 | 6.46x time, 3.00x bytes, +2 allocs |

The registry is a correctness and governance feature, not a raw callback
fast path. A measured result-ownership optimization reduced the unoptimized
no-authorizer probe from 144.7 ns/48 B/3 allocs to 114.8 ns/32 B/2 allocs,
and the authorized probe from 187.3 ns/64 B/4 allocs to 162.5 ns/48 B/3
allocs. The extra boundary cost is retained because it provides versioning,
authorization, caller-input isolation, bounded results, and panic isolation.
