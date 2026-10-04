# Materialize-style source schema registry

`hatSchema.SourceSchemaRegistry` is an opt-in registry for CDC or other
producers that attach a source name, monotonically increasing version, and
schema fingerprint to each event. It keeps a bounded history and validates a
new definition once at registration time. Repeated event validation then uses
the version and fingerprint only.

## Default policy

`NewSourceSchemaRegistry(SourceSchemaRegistryOptions{})` uses:

- at most 256 named sources;
- at most 64 versions per source;
- `SourceSchemaCompatibilityRolling`, which allows the existing conservative
  rolling-schema rules: nullable columns may be appended and existing
  nullability may be relaxed, while removals, reorderings, type changes, and
  required columns are rejected.

The registry does not change existing schema validation or ingestion defaults.
Use `SourceSchemaCompatibilityAny` only when compatibility is enforced by an
external contract.

## Example

```go
registry, err := hatSchema.NewSourceSchemaRegistry(hatSchema.SourceSchemaRegistryOptions{})
if err != nil {
    return err
}

registered, err := registry.Register(orderSource, 1)
if err != nil {
    return err
}

// Store registered.Fingerprint with the producer's event metadata.
if err := registry.Validate("orders", event.SchemaVersion, event.SchemaFingerprint); err != nil {
    return err
}
```

`Register` is idempotent for the same source, version, and fingerprint. A
different definition at an existing version is rejected. `Lookup` and
`Snapshot` return deep copies, so callers cannot mutate retained definitions.
Registration is serialized; successful `Validate` calls are read-locked and
allocation-free. History is monotonic per source and bounded by the configured
limit.

## Measured tradeoff

The contract benchmark compares repeated full compatibility checks with the
steady-state registry validation path on the same three-column `orders` source.
Values are medians of three 100 ms runs on the local AMD Ryzen 9 5950X:

| Path | Time | Heap | Allocs |
| --- | ---: | ---: | ---: |
| Full `CheckRollingCompatibility` | 897.6-924.8 ns/op | 224 B/op | 3/op |
| Registry `Validate` hot path | 11.64-11.78 ns/op | 0 B/op | 0/op |
| First registration, including registry creation | 1,274-1,345 ns/op | 1,160 B/op | 20/op |
| Snapshot of 64 retained versions | 8,954-9,370 ns/op | 21,120 B/op | 65/op |

That is approximately 77x lower CPU for repeated validation and removes the
three hot-path allocations. The cost is retained cloned schema history and
snapshot copying. Bounds make that cost explicit; callers with no need for
history should keep using direct schema validation. Raw benchmark output is in
`BENCHMARK.md`.

## Security and operational boundaries

The registry stores schema definitions and fingerprints, not event payloads or
secrets. It does not authenticate fingerprints, persist history, or wire itself
into CDC ingestion. A transport or durable catalog must provide authentication,
durability, replay policy, and source authorization. Invalid, unknown, stale,
conflicting, or mismatched versions fail closed.
