# M-U20: Secret And Connection Resources

## Status

Implemented as an opt-in `hatAuth.ResourceRegistry`. The registry keeps
secret values in memory, exposes redacted connection metadata, and applies
explicit ownership and reader checks. It does not replace an external secret
manager or durable backup format.

## Why

SQL and peer-facing applications commonly need named connections without
putting credentials in connection names, endpoint strings, logs, snapshots,
or error messages. A small reusable registry gives callers one bounded
contract for creation, access, rotation, and deletion while leaving transport
and secret-manager integration in the caller's control.

## API Shape

```go
registry, err := hatAuth.NewResourceRegistry(hatAuth.ResourceRegistryOptions{})

_, err = registry.CreateSecret(hatAuth.SecretSpec{
	Name:    "orders-db-secret",
	Owner:   "billing-service",
	Readers: []string{"billing-service", "orders-reader"},
	Value:   []byte("password-from-secret-manager"),
})

_, err = registry.CreateConnection(hatAuth.ConnectionSpec{
	Name:       "orders-db",
	Owner:      "billing-service",
	Readers:    []string{"billing-service", "orders-reader"},
	Driver:     "postgres",
	Endpoint:   "db.internal:5432",
	Database:   "orders",
	SecretName: "orders-db-secret",
})

metadata, err := registry.Connection("orders-db", "orders-reader")
resolved, err := registry.ResolveConnection("orders-db", "orders-reader")
password := resolved.SecretValue()
```

`Connection` returns metadata only. `ResolveConnection` is the explicit
credential boundary and returns a copy of the secret through
`ResolvedConnection.SecretValue()`. Callers should clear that copy after use.

Secret versions start at one. Rotation requires the owner and an exact
`expectedVersion`, so a stale operator cannot overwrite a newer value:

```go
nextVersion, err := registry.RotateSecret(
	"orders-db-secret", "billing-service", metadata.SecretVersion,
	[]byte("new-password"),
)
```

Deleting a secret referenced by a connection returns `ErrResourceInUse`.
Delete and rotate operations also use an exact version check where applicable.

## Bounds And Defaults

Zero-valued options select these defaults:

| Limit | Default |
|---|---:|
| Secrets | 1,024 |
| Connections | 1,024 |
| Secret bytes per value | 1 MiB |
| Name bytes | 256 |
| Principal bytes | 256 |
| Readers per resource | 64 |

All limits are configurable. Negative limits are rejected. Names and
principals reject empty values, control characters, and NUL bytes. Reader
lists are deduplicated, sorted, and bounded. A connection reader must also be
allowed by the referenced secret; this prevents a connection from widening
secret access.

Endpoints are deliberately metadata-only and reject userinfo and common
credential query keys such as `password`, `secret`, `token`, and `api_key`.
They are not a URL parser or a network dialer.

## Redaction And Persistence

The following paths do not expose secret values:

- `Connection` and `SecretMetadata` return metadata only.
- `ResourceRegistry.Snapshot()` contains secret names, owners, readers, and
  versions, but no secret bytes.
- `String`, `GoString`, and diagnostic formatting for registry resources are
  redacted.
- `SecretSpec.Value` is excluded from JSON through `json:"-"`.

Snapshots are suitable for non-secret metadata persistence and audit. A
durable restart must reload values from an external secret manager, then
recreate the registry and connections. Do not put `SecretSpec.Value` in a
general application snapshot or log.

## Correctness And Security Coverage

Focused tests cover:

- create, read, rotate, delete, and connection lifecycle behavior;
- owner, reader, and referenced-secret authorization;
- stale-version rejection and secret-in-use deletion rejection;
- bounded resource counts, values, names, principals, and reader lists;
- endpoint credential rejection and control-character rejection;
- snapshot metadata redaction and value-copy isolation;
- redacted `fmt` output;
- concurrent reads, rotation, and metadata access under the race detector.

Commands used for verification:

```text
make format-mu020-secret-resources
make test-mu020-secret-resources
make test-mu020-package
make race-mu020-secret-resources
make vet-mu020-secret-resources
make benchmark-mu020-secret-resources
```

## Benchmark

The baseline is a raw map lookup with no authorization or copy boundary. The
implemented path is intentionally opt-in and measures policy plus redaction,
not a replacement for a hot query lookup:

| Path | Median ns/op | B/op | allocs/op | Relative to raw map |
|---|---:|---:|---:|---:|
| Baseline raw connection lookup | 10.09 | 0 | 0 | 1.00x |
| Registry metadata lookup | 90.77 | 16 | 1 | 9.00x slower |
| Registry credential resolution | 121.0 | 32 | 2 | 12.0x slower |

Raw five-sample output from the paired run:

```text
BenchmarkMU020BeforeConnectionLookup   9.969 ns/op  0 B/op  0 allocs/op
BenchmarkMU020BeforeConnectionLookup  10.09 ns/op   0 B/op  0 allocs/op
BenchmarkMU020BeforeConnectionLookup  10.80 ns/op   0 B/op  0 allocs/op
BenchmarkMU020BeforeConnectionLookup  10.41 ns/op   0 B/op  0 allocs/op
BenchmarkMU020BeforeConnectionLookup   9.847 ns/op  0 B/op  0 allocs/op
BenchmarkMU020AfterConnectionLookup    90.77 ns/op  16 B/op  1 allocs/op
BenchmarkMU020AfterConnectionLookup    91.11 ns/op  16 B/op  1 allocs/op
BenchmarkMU020AfterConnectionLookup    87.03 ns/op  16 B/op  1 allocs/op
BenchmarkMU020AfterConnectionLookup    92.87 ns/op  16 B/op  1 allocs/op
BenchmarkMU020AfterConnectionLookup    87.31 ns/op  16 B/op  1 allocs/op
BenchmarkMU020AfterConnectionResolve  123.5 ns/op  32 B/op  2 allocs/op
BenchmarkMU020AfterConnectionResolve  121.0 ns/op  32 B/op  2 allocs/op
BenchmarkMU020AfterConnectionResolve  114.6 ns/op  32 B/op  2 allocs/op
BenchmarkMU020AfterConnectionResolve  121.9 ns/op  32 B/op  2 allocs/op
BenchmarkMU020AfterConnectionResolve  116.1 ns/op  32 B/op  2 allocs/op
```

The tradeoff is explicit: metadata access adds about 81 ns and one small
allocation versus a raw map, while credential resolution adds about 111 ns
and one additional allocation for copies. In exchange, callers get bounded
authorization, versioned rotation, redaction, and value isolation. Keep
resolution outside per-row SQL loops and cache only short-lived, already
authorized client state when the surrounding application requires it.

## Scope Boundary

This primitive does not implement network dialing, TLS certificate storage,
secret-manager APIs, SQL `CREATE CONNECTION` syntax, cluster replication, or
durable secret backup. Those layers can use the registry while preserving the
same redaction and authorization boundary.
