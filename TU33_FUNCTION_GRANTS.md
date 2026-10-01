# Function Grants

`hatAuth` supports function-scoped authorization rules, inspired by
Tarantool's function grants. A rule can restrict a principal to exact function
names or to a trailing-`*` prefix, optionally combined with namespaces.

```go
policy := hatAuth.Policy{
	Principals: map[string][]string{"operator": {"maintenance"}},
	Roles: []hatAuth.Role{{Name: "maintenance", Rules: []hatAuth.Rule{{
		Functions:  []string{"orders.refresh", "orders.rebuild:*"},
		Namespaces: []string{"tenant-eu:*"},
	}}}},
}

allowed := policy.AuthorizeFunction("operator", "orders.refresh", "tenant-eu:orders")
```

The equivalent explicit request is:

```go
allowed := policy.AuthorizeRequest("operator", hatAuth.AuthorizationRequest{
	Function:  "orders.refresh",
	Namespace: "tenant-eu:orders",
})
```

`RoleCatalog.AuthorizeFunction` applies the same check using the versioned,
owned role catalog. Function selectors are copied into role grants and
deterministic snapshots. Malformed or control-character function names are
rejected by the catalog.

Function selectors fail closed for the legacy `Authorize` method when a rule
contains `Functions`; callers that invoke stored procedures must pass the
function name through `AuthorizeFunction` or `AuthorizationRequest`. The
library does not execute or sandbox functions, and the feature is opt-in: an
empty legacy policy remains disabled for compatibility, while an empty
`RoleCatalog` denies access.

## Measurement

Linux amd64, AMD Ryzen 9 5950X, `go test -benchmem -count=5`:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Parent commit legacy authorization | 83.5 average | 0 | 0 |
| Current legacy authorization | 83.2 average | 0 | 0 |
| Current function authorization | 68.2 average | 0 | 0 |

The legacy path changed by roughly -0.3 ns/op (-0.4%) in this run and retained
zero allocations; the difference is within normal benchmark noise. The
function-path number is not a direct comparison with the legacy benchmark
because it uses fewer selectors; it is a capacity reference for the new API.

Focused verification:

```text
make test-t-u33-function-grants
make verify-t-u33-function-grants
```
