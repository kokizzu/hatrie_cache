# M-G45: Named Connection and Secret Resources

`hat/hatResource` provides an importable, bounded registry for named
connections and secrets. The design adopts Materialize-style named resources
and rotation while keeping the feature opt-in and independent of existing SQL
and transport configuration.

## Security Contract

- Every resource has a namespace and exact owner.
- Secret reads, verification, rotation, and connection resolution require the
  owning principal.
- A connection may reference an existing secret only when both owners match.
- Secret metadata snapshots never contain secret values or connection options.
- Rotation increments a version and can retain the previous value only for a
  bounded grace period. `VerifySecret` compares current and previous values
  with constant-time comparison.
- Registry cardinality, names, endpoint length, option count, and secret size
  are bounded. Existing paths do not create a registry automatically.

## Defaults

| Limit | Default |
| --- | ---: |
| Named secrets | 1,024 |
| Named connections | 1,024 |
| Secret value | 64 KiB |
| Namespace/name/owner | 128 bytes |
| Rotation grace maximum | 365 days |

Zero-valued `RegistryOptions` selects these defaults. Callers can lower them
for a tenant or test registry.

## Example

```go
registry, err := hatResource.NewRegistry(hatResource.RegistryOptions{})
if err != nil {
	return err
}

secretRef := hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"}
_, err = registry.PutSecret(hatResource.SecretSpec{
	Ref: secretRef, Owner: "tenant-a", Value: password,
})
if err != nil {
	return err
}

_, err = registry.PutConnection(hatResource.ConnectionSpec{
	Ref:      hatResource.ResourceRef{Namespace: "tenant-a", Name: "primary"},
	Owner:    "tenant-a",
	Driver:   "postgres",
	Endpoint: "db.internal:5432",
	Secret:   secretRef,
})
if err != nil {
	return err
}

resolved, err := registry.ResolveConnectionSecret(
	hatResource.ResourceRef{Namespace: "tenant-a", Name: "primary"},
	"tenant-a",
)
if err != nil {
	return err
}
defer func() { resolved.Value = "" }()
```

The caller owns the lifetime of the returned secret string and must not log or
persist it. `Snapshot` is safe for diagnostics because it returns metadata
only. Connection options are copied on resolve and are intentionally excluded
from snapshots; callers must keep credentials out of those non-secret fields.

## Measured Tradeoff

Five benchmark samples were collected on AMD Ryzen 9 5950X, linux/amd64, with
`go test -run '^$' -bench '^BenchmarkMG45' -benchmem -count=5`.

Raw samples in `ns/op`:

```text
DirectMapLookupBaseline       7.822  8.821  8.795  7.729  7.478
RegistryCreateAndRegister   371.9  356.3  360.0  362.3  362.3
RegistryResolveSecret        67.99  68.76  68.26  69.41  72.26
RegistryVerifySecret         68.36  71.51  73.72  73.67  76.20
RegistryResolveConnection   102.2  113.0  95.92 100.7  111.3
```

| Operation | Median ns/op | B/op | allocs/op | CPU vs direct lookup |
| --- | ---: | ---: | ---: | ---: |
| Direct map lookup | 7.822 | 0 | 0 | 1.00x |
| Create and register | 362.3 | 704 | 5 | 46.3x |
| Resolve secret | 68.76 | 0 | 0 | 8.79x |
| Verify secret | 73.67 | 0 | 0 | 9.42x |
| Resolve connection | 102.2 | 0 | 0 | 13.1x |

The registry is therefore appropriate for connector setup, credential
rotation, and control-plane checks. It is deliberately not wired into a hot
per-row lookup path. Reuse the registry and retain only authorized handles or
resolved values for the shortest practical lifetime.

## Verification

Focused tests cover owner isolation, metadata redaction, rotation grace and
expiry, connection references, cross-owner rejection, capacity/input bounds,
concurrent resolution, race detection, and `go vet`.
