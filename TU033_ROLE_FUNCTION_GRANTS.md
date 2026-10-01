# T-U33 Role-Based Function Grants

T-U33 adopts the Tarantool-style idea of function-level grants through the
existing bounded `hatAuth.RoleCatalog`. It adds
`RoleCatalogStoredFunctionAuthorizer`, an adapter for
`hatAuth.StoredFunctionRegistry`.

## Contract

- A function grant is an existing role `Rule` with an object selector of
  `function:<name>`.
- `REGISTER` and `CALL` are the only accepted stored-function operations.
- A nil catalog, empty principal/name, unknown operation, missing role, or
  missing object grant fails closed.
- Authorization is evaluated on every register/call. Revoking a grant takes
  effect on the next operation without cache invalidation or restart.
- Role and grant mutation keeps the existing monotone catalog version and
  exact-version fencing behavior.
- The adapter allocates nothing on the authorization path and adds no cost to
  callers that continue using the legacy policy callback or no registry.

## Example

```go
catalog, _ := hatAuth.NewRoleCatalog(hatAuth.RoleCatalogOptions{})
catalog.CreateRole("owner", hatAuth.RoleSpec{Name: "operators", Owner: "owner"})
catalog.GrantRole("owner", "alice", "operators")
catalog.Grant("owner", hatAuth.RoleGrantSpec{
    Role: "operators",
    Rule: hatAuth.Rule{
        Commands: []string{
            hatAuth.StoredFunctionOperationRegister,
            hatAuth.StoredFunctionOperationCall,
        },
        Objects: []string{"function:echo"},
    },
})

registry, _ := hatAuth.NewStoredFunctionRegistry(
    hatAuth.StoredFunctionRegistryOptions{
        Authorize: hatAuth.RoleCatalogStoredFunctionAuthorizer(catalog),
    },
)
_, _ = registry.Register("alice", hatAuth.StoredFunctionSpec{
    Name: "echo", Version: 1,
    Handler: func(ctx context.Context, input []byte) ([]byte, error) {
        return input, nil
    },
})
```

The registry still owns function versioning, payload bounds, context
propagation, panic isolation, and input/output ownership. The role catalog
owns role membership, grant lifecycle, and authorization state.

## Measured Cost

The benchmark compares the direct catalog request used as the baseline with
the function-specific adapter. Each result is from five `-benchmem` samples on
the same AMD Ryzen 9 5950X host.

| Path | Samples (ns/op) | Median | Bytes/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Direct `RoleCatalog.Authorize` baseline | 131.3, 129.9, 128.6, 127.4, 136.2 | 129.9 | 0 | 0 |
| Adapter baseline after implementation | 128.2, 127.3, 127.4, 129.8, 128.3 | 128.2 | 0 | 0 |
| `RoleCatalogStoredFunctionAuthorizer` | 159.5, 158.2, 158.4, 160.5, 159.1 | 159.1 | 0 | 0 |

The adapter is `1.24x` the post-implementation baseline, or about `30.9 ns`
and `24.1%` extra CPU per authorization, with no additional allocation or
retained memory. The cost buys canonical operation/name validation and an
object-prefix contract while preserving immediate revocation. Because the
feature is opt-in, this tradeoff does not affect existing default paths.

## Deliberate Scope

This adapter does not add a second role database, implicit role creation,
distributed authorization, or a cached permission result. Those concerns would
weaken revocation semantics or duplicate the existing catalog contract.
