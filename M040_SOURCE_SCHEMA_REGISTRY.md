# M-U40 Source Schema Registry

`hatSchema.SourceSchemaRegistry` is an opt-in admission and rollback registry
for CDC or connector schema metadata. It closes the gap between a schema
fingerprint exchanged during discovery and the version that a source is
allowed to activate.

The existing CDC, Avro, Protobuf, SQL, and discovery paths remain unchanged
unless a caller explicitly creates and uses this registry.

## What It Retains

The registry stores only:

- source identity;
- positive monotonic version numbers;
- bounded fingerprints; and
- a bounded number of retained versions per source.

It never stores rows, payloads, raw Avro/Protobuf schemas, credentials, or
format-specific parser errors. Format-specific bytes stay in the caller's
existing bounded registry/cache.

## Safe Defaults

`NewSourceSchemaRegistry(SourceSchemaRegistryOptions{})` uses:

- at most `1024` source histories;
- at most `8` retained versions per source; and
- `SourceSchemaCompatibilityExact`.

Exact mode accepts a new version only when its fingerprint is unchanged. A
caller must explicitly select backward, forward, or full compatibility and
provide `ValidateCompatibility` before a changed fingerprint can be
activated. This makes an accidentally incomplete compatibility integration
fail closed.

## Lifecycle

```go
import "hatrie_cache/hat/hatSchema"

registry, err := hatSchema.NewSourceSchemaRegistry(
	hatSchema.SourceSchemaRegistryOptions{},
)
if err != nil {
	panic(err)
}

v1 := hatSchema.SourceSchemaVersion{
	Source: "orders",
	Version: 1,
	Fingerprint: "sha256:orders-v1",
}
if _, err := registry.Activate(v1); err != nil {
	panic(err)
}

candidate := hatSchema.SourceSchemaVersion{
	Source: "orders",
	Version: 2,
	Fingerprint: "sha256:orders-v2",
}
if result, err := registry.Validate(candidate); err == nil && result.Accepted {
	// Publish only after the caller has cut over the source decoder.
	_, err = registry.Activate(candidate)
}

_, err = registry.Rollback("orders", 1)
```

`Validate` does not mutate state. `Activate` validates and publishes under one
registry lock; repeating the active version with the same fingerprint is
idempotent. Forward activation rejects stale versions and conflicting reuse of
an already-retained version. `Rollback` can select only a retained version
that was previously admitted. The oldest non-active history is evicted when
the per-source bound is reached; setting the bound to `1` intentionally
disables rollback.

## Format-Specific Compatibility

The generic registry does not guess compatibility from a fingerprint. Adapt a
format-specific validator through the callback:

```go
registry, err := hatSchema.NewSourceSchemaRegistry(
	hatSchema.SourceSchemaRegistryOptions{
		CompatibilityMode: hatSchema.SourceSchemaCompatibilityBackward,
		ValidateCompatibility: func(previous, candidate hatSchema.SourceSchemaVersion, mode hatSchema.SourceSchemaCompatibilityMode) error {
			previousBytes := loadAvroSchemaByFingerprint(previous.Fingerprint)
			candidateBytes := loadAvroSchemaByFingerprint(candidate.Fingerprint)
			return validateAvroCompatibility(previousBytes, candidateBytes, mode)
		},
	},
)
```

The callback receives metadata only. Its error is converted to the stable
`ErrSourceSchemaIncompatible` result and is not wrapped or logged by the
registry. This prevents raw schema text or parser details from entering
operational diagnostics.

## Diagnostics And Recovery

Use `SourceSchemaValidationResult` for safe logs and metrics. It contains the
source, versions, acceptance flag, and a stable reason such as `stale version`
or `incompatible schema`, but no schema bytes or fingerprints. Use
`errors.Is` with the exported sentinel errors for programmatic handling.

`Snapshot` returns sorted source histories and copied version slices. `Stats`
reports source count, retained-version count, activations, rollbacks, and
rejections. Both methods are safe while connectors are active.

## Measured Cost

The registry is deliberately not inserted into the row or message hot path.
The focused benchmark compared the old direct metadata equality check with the
new opt-in registry calls on Linux/amd64, AMD Ryzen 9 5950X, five samples per
benchmark. See [BENCHMARK.md](BENCHMARK.md#m-u40-source-schema-registry) for
the raw samples.

| Operation | Median ns/op | B/op | allocs/op | Relative to direct check |
| --- | ---: | ---: | ---: | ---: |
| Direct metadata equality | 3.699 | 0 | 0 | 1.0x |
| `Validate` | 66.49 | 0 | 0 | 18.0x |
| Same-version `Activate` | 75.72 | 0 | 0 | 20.5x |

The constructor-specific benchmark also compared the lazy source map with an
isolated eager map allocation using the default capacity of `1024` sources:

| Constructor path | Median ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | ---: |
| Lazy `NewSourceSchemaRegistry` | 76.76 | 160 | 2 | baseline |
| Eager map allocation baseline | 6,899 | 54,608 | 5 | 89.9x slower, 341x more bytes |

The eager row isolates the map allocation rather than reproducing the complete
pre-change constructor, so it is evidence for the allocation choice, not a
claim about a full historical release benchmark.

This overhead is acceptable for schema transitions, which are control-plane
events, but it would be inappropriate to call the registry for every CDC row.

## Verification

The M-U40 focused targets cover activation, idempotency, version conflicts,
bounded eviction, rollback, invalid metadata, secret-safe callback errors,
copy-safe snapshots, concurrent admission, race detection, vet, and the
benchmark above:

```text
make format-m040
make test-m040
make race-m040
make vet-m040
make benchmark-m040
```
