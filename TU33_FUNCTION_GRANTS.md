# T-U33 Function Grants

`hatAuth.RoleCatalog` now supports an optional function dimension on a grant.
This is a small least-privilege extension for embedded services that expose
stored procedures or named functions alongside spaces and SQL sources.

## Contract

Set `Rule.Functions` to exact function names or trailing-prefix selectors such
as `math.*`. Supply the canonical function identity in
`AuthorizationRequest.Function` when authorizing an invocation.

- Empty `Functions` keeps the existing unrestricted-selector behavior.
- A function selector never matches an empty function identity.
- Selectors are bounded, deduplicated, sorted, and reject embedded wildcards
  and control characters.
- Existing command, namespace, source, and object selectors continue to be
  checked together with the function selector.
- Snapshot and restore preserve function selectors without sharing mutable
  slices with the caller.

The role owner remains responsible for granting the rule. A function grant is
not automatically attached to an HTTP, gRPC, SQL, or stored-procedure
endpoint; the embedding service must authenticate the principal, canonicalize
the function identity, and call `RoleCatalog.Authorize`.

Rules without a function selector retain the legacy `hatAuth.Policy` behavior.
When a caller uses the shared `Rule` type with `Policy.AuthorizeRequest`, a
function selector is enforced there as well; the older convenience methods do
not have a function argument and therefore cannot satisfy a function grant.
This feature does not add explicit deny rules or a function catalog; those
policies remain caller-owned.

## Measurement

The benchmark uses five samples on the same AMD Ryzen 9 5950X host. The
existing RoleCatalog benchmark from the unchanged `origin/master` worktree
was 178.3 ns/op, 0 B/op, and 0 allocs/op at the median. The paired post-change
benchmarks were:

| Path | Median | Memory | Relative |
| --- | ---: | ---: | ---: |
| RoleCatalog namespace grant, no function selector | 168.5 ns/op | 0 B/op, 0 allocs/op | 1.00x |
| RoleCatalog trailing-prefix function grant | 180.5 ns/op | 0 B/op, 0 allocs/op | 1.07x |

The function selector costs about 12 ns/op in this fixture. Because
`RoleCatalog` is opt-in and the legacy `Policy` path is untouched, there is no
default request-path cost. The measured cost is accepted for the narrower
authorization boundary.
