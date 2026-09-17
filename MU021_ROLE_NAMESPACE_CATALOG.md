# M-U21: Role And Namespace Catalog

## Status

Implemented as the opt-in `hatAuth.RoleCatalog`. It adds bounded role
ownership, parent-role inheritance, namespace ownership, scoped grants,
principal membership, exact-version mutation checks, and deterministic
snapshot/restore. The existing `hatAuth.Policy` remains unchanged and keeps
its legacy compatibility behavior.

## Basic Usage

```go
catalog, err := hatAuth.NewRoleCatalog(hatAuth.RoleCatalogOptions{})

_, err = catalog.CreateNamespace("admin", hatAuth.NamespaceSpec{
	Name:  "tenant-eu",
	Owner: "admin",
})
_, err = catalog.CreateNamespace("admin", hatAuth.NamespaceSpec{
	Name:   "tenant-eu/orders",
	Parent: "tenant-eu",
	Owner:  "admin",
})

_, err = catalog.CreateRole("admin", hatAuth.RoleSpec{
	Name:  "reader",
	Owner: "admin",
})
_, err = catalog.CreateRole("admin", hatAuth.RoleSpec{
	Name:    "analyst",
	Owner:   "admin",
	Parents: []string{"reader"},
})
_, err = catalog.Grant("admin", hatAuth.RoleGrantSpec{
	Role: "reader",
	Rule: hatAuth.Rule{
		Commands:   []string{"SELECT"},
		Namespaces: []string{"tenant-eu"},
		Sources:    []string{"orders"},
	},
})
_, err = catalog.GrantRole("admin", "alice", "analyst")

allowed := catalog.Authorize("alice", hatAuth.AuthorizationRequest{
	Command:   "SELECT",
	Namespace: "tenant-eu/orders",
	Source:    "orders",
})
```

`alice` inherits `reader` through `analyst`. A namespace grant to
`tenant-eu` matches that namespace and descendants such as
`tenant-eu/orders`. The authorization matcher also understands the existing
colon-delimited child form, such as `tenant-eu:orders`, for compatibility with
older namespace selectors.

The catalog is default-deny: an empty catalog, unknown principal, missing
request dimension, unmatched role, or unmatched selector is denied. Empty
selector lists mean unrestricted for that dimension, so a grant with no
selectors is intentionally broad and should be reserved for a controlled
role owner.

## Ownership And Mutation

- A role creator must be its owner and must own every parent role.
- A namespace creator must be its owner and must own its parent namespace.
- Only a role owner can grant or revoke that role's memberships and rules.
- A scoped grant additionally requires the grantor to own every referenced
  namespace.
- Membership and grant deletion use the current catalog `Version()` as an
  exact compare-and-swap token. Stale writers receive
  `ErrRoleCatalogConflict`.
- Roles with children, memberships, or grants cannot be deleted.
- Namespaces with children or scoped grants cannot be deleted.
- Role ownership is management authority; membership does not imply
  ownership.

The version increments after every successful create, grant, membership, and
delete operation. Failed authorization, duplicate, limit, and stale-version
operations do not mutate the catalog.

## Snapshot And Restore

`RoleCatalog.Snapshot()` returns sorted slices and deep copies of roles,
namespaces, grants, and memberships. `Restore(snapshot, expectedVersion)`
validates bounds, duplicate records, parent existence, ownership, role cycles,
namespace paths, grant references, and version fencing before replacing the
catalog state.

The snapshot is suitable for caller-managed durable policy storage. It does
not include runtime locks or hidden maps. Persist it atomically with the
application's policy checkpoint and use the catalog version as the fencing
value.

## Bounds And Defaults

Zero-valued options select these defaults:

| Limit | Default |
|---|---:|
| Roles | 1,024 |
| Namespaces | 4,096 |
| Grants | 16,384 |
| Principal memberships | 16,384 |
| Parents per role | 16 |
| Selectors per rule dimension | 64 |
| Name bytes | 256 |
| Principal bytes | 256 |

All configured limits are bounded and negative values are rejected. Resource
names, principals, selectors, and authorization request dimensions reject
invalid UTF-8 and control characters. Role names cannot contain `/`; namespace
hierarchies use `/` and reject `.`/`..` path segments.

## Security Boundary

The catalog does not authenticate principals, issue tokens, execute SQL, or
replicate itself. Callers must obtain the principal from an existing trusted
identity provider and must route every protected operation through
`Authorize` or the ownership checks. Legacy policy users are unaffected until
they explicitly construct and use a `RoleCatalog`.

Strict request validation rejects control characters before trimming, avoiding
authorization bypasses such as treating `alice\n` as `alice`. Object and
source selectors cannot be bypassed with an empty request dimension.

## Verification

The focused test suite covers inheritance, descendant namespaces, default
deny, ownership, duplicate and limit handling, exact-version conflicts,
deletion dependencies, snapshot deep-copy isolation, restore fidelity,
malformed cycles, control-character rejection, and concurrent authorization
and snapshots.

```text
make format-mu021-role-catalog
make test-mu021-role-catalog
make benchmark-mu021-role-catalog
```

## Benchmark And Tradeoff

The benchmark uses `GOMAXPROCS=1`, `-benchtime=500ms`, and `-count=5` on an
AMD Ryzen 9 5950X Linux/amd64 host. It compares the existing flat `Policy`
authorization with the final catalog's direct-role fast path. Both paths use
the same command, namespace, and source dimensions and report zero heap
allocations.

Pre-implementation baseline (`make benchmark-mu021-role-catalog-baseline`):

```text
78.96 ns/op   0 B/op   0 allocs/op
81.10 ns/op   0 B/op   0 allocs/op
83.29 ns/op   0 B/op   0 allocs/op
83.49 ns/op   0 B/op   0 allocs/op
76.76 ns/op   0 B/op   0 allocs/op
```

Final secure paired run:

```text
Before: 81.24 ns/op   0 B/op   0 allocs/op
Before: 85.24 ns/op   0 B/op   0 allocs/op
Before: 81.45 ns/op   0 B/op   0 allocs/op
Before: 78.87 ns/op   0 B/op   0 allocs/op
Before: 81.43 ns/op   0 B/op   0 allocs/op
After:  180.8 ns/op   0 B/op   0 allocs/op
After:  170.8 ns/op   0 B/op   0 allocs/op
After:  176.6 ns/op   0 B/op   0 allocs/op
After:  175.0 ns/op   0 B/op   0 allocs/op
After:  177.8 ns/op   0 B/op   0 allocs/op
```

| Path | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Existing flat `Policy` control | 81.43 | 0 | 0 | 1.00x |
| Final secure `RoleCatalog` direct role | 176.6 | 0 | 0 | 2.17x slower |

An attempted trim-before-validation optimization was reverted after a
regression test showed that `alice\n` could be treated as `alice`; strict
principal and request validation remains in the final path. The final catalog
costs about 95 ns over the paired flat policy, but it provides mutable,
concurrent, owned, hierarchical policy semantics without heap allocation. Use
it at authorization boundaries and keep the legacy `Policy` path for callers
that need its lower-complexity static behavior.
