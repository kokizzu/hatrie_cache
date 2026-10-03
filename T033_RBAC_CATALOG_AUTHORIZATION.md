# T-U33 RBAC Catalog Authorization

T-U33 adds the missing enforcement boundary for the existing role and space
catalog. It is deliberately opt-in and keeps the empty legacy policy behavior
unchanged.

## Runtime behavior

`hatAuth.Authorizer` evaluates the legacy `Policy` first and then the optional
`RoleCatalog`. If both are configured, both must allow the request. A nil
catalog adds no work or restrictions. A non-empty catalog is default-deny for
unassigned principals and unmatched grants.

The HTTP monitoring handler and the gRPC cache server both use the composed
authorizer for cache commands and SQL source checks. Internal replication
authentication remains a separate boundary and is not widened by the catalog.

## Static configuration

The cache binary accepts a JSON `RoleCatalogSnapshot` through:

```text
-monitoring-auth-token alice -rbac-catalog /etc/hatrie/rbac-catalog.json
```

The snapshot is loaded with unknown-field rejection, one-JSON-value
validation, and the catalog's existing role, namespace, grant, membership,
limit, and hierarchy checks. Authentication is required whenever the catalog
file is configured. The file contains grant metadata and is not a secret; file
permissions should still restrict who can change authorization.

The snapshot can be produced by an importing Go caller with
`catalog.Snapshot()` and serialized as JSON. Catalog mutation and safe rollout
remain caller-owned so a process does not expose an unreviewed authorization
management endpoint.

## Benchmark

The benchmark uses one authorized request and reports the existing direct
policy/catalog paths beside the composed authorizer. The catalog path is a
security feature, not a CPU optimization; its acceptance criterion is bounded
overhead with zero per-request allocations.

| Path | Median ns/op | B/op | allocs/op | Relative to direct catalog |
|---|---:|---:|---:|---:|
| Legacy policy only | 76.42 | 0 | 0 | 0.49x |
| Role catalog only | 155.1 | 0 | 0 | 1.00x |
| Catalog authorizer | 158.6 | 0 | 0 | 1.02x |
| Composed policy + catalog | 220.6 | 0 | 0 | 1.42x |

Run the focused benchmark with the repository's Makefile target used for this
feature, then replace the measured rows in `BENCHMARK.md` if hardware or Go
version changes.
