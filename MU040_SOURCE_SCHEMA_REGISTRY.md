# M-U40 Source Schema Registry

`hatSchema.SchemaRegistry` is an opt-in source/connector schema validation
boundary. It tracks a bounded version history per named subject and checks a
candidate schema against the latest registered version before publication.

## Usage

```go
registry, err := hatSchema.NewSchemaRegistry(hatSchema.SchemaRegistryOptions{
	Policy: hatSchema.SchemaRegistryPolicyRolling,
})
if err != nil {
	return err
}

report, err := registry.Register("postgres/orders", schemaV1)
if err != nil {
	return err
}

report, err = registry.Register("postgres/orders", schemaV2)
if errors.Is(err, hatSchema.ErrSchemaRegistryIncompatible) {
	// Keep the connector on the previous schema and inspect report.Changes.
	return fmt.Errorf("schema rejected: %v", report.Changes)
}
if err != nil {
	return err
}
```

`Check` performs the same validation without publishing. `Current`, `History`,
and `Subjects` return detached values, so callers cannot mutate registry state.
Repeated registration of the same version and fingerprint is idempotent.

## Policies And Bounds

- The default policy is `SchemaRegistryPolicyStrict`; it accepts only an
  identical schema fingerprint at a new version.
- `SchemaRegistryPolicyRolling` delegates compatibility decisions to
  `CheckRollingCompatibility`. The conservative rolling rules allow nullable
  column additions and relaxing `NOT NULL`; type changes, removals, reordering,
  required additions, source changes, and constraint changes are rejected.
- The default bound is 256 subjects with 16 retained versions per subject.
  `MaxSubjects` and `MaxVersionsPerSubject` can lower or raise those values
  within hard safety limits. When a subject history is full, the oldest entry
  is evicted after a successful new registration.
- The registry does not apply a schema to a connector, persist itself, or
  authorize a connector. CDC and source integrations call `Check` or
  `Register` at their schema-change boundary and own durable storage and
  rollback.

## Measured Cost

Benchmark: `make benchmark-mu040-source-schema-registry` on an AMD Ryzen 9 5950X, Go benchmark
`-count=3`, `-benchmem`, 2026-10-03.

| Path | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Direct `CheckRollingCompatibility` | 766.4 | 224 | 3 |
| `SchemaRegistry.Check` | 1,321 | 1,120 | 6 |

The registry check is about 1.72x slower, uses 5.00x the transient bytes, and
uses 2.00x the allocations in this small schema fixture. This is validation
control-plane cost only; existing callers that do not construct a registry have
no added query or connector-path work.
