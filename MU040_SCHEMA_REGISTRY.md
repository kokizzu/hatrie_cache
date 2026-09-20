# M-U40 Source Schema Registry

`hatSql.SQLSchemaRegistry` is an opt-in, bounded in-memory registry for
versioned source schemas. It borrows the schema-evolution boundary used by
Materialize-style CDC connectors without requiring a network registry or
changing ordinary ingestion.

## Usage

```go
registry, err := hatSql.NewSQLSchemaRegistry(hatSql.SQLSchemaRegistryOptions{
	Compatibility: hatSql.SQLSchemaCompatibilityFull,
})
if err != nil {
	return err
}

err = registry.Register(hatSql.SQLSchemaDefinition{
	Source:  "events",
	Version: "v1",
	Columns: []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "region", Type: hatSql.SQLRowBinaryString, Nullable: true},
	},
})
if err != nil {
	return err
}

coordinator := hatSql.NewSQLSourceIngestionCoordinatorWithSchemaRegistry(registry)
_, err = coordinator.Ingest(hatSql.SQLSourceIngestion{
	Source:        "events",
	SchemaVersion: "v1",
	Transaction: hatSql.SQLSourceTransaction{
		ID: "tx-1",
		Offsets: []hatSql.SQLSourceOffset{{
			Source: "events", Partition: "0", Offset: 1,
		}},
	},
}, applyTransaction)
```

The coordinator validates the source/version pair before calling
`applyTransaction`. An unknown or malformed version is rejected and the
callback is not invoked. Schema versions are copied into `Snapshot` and
`SnapshotEnvelopes`, so restore/replay validates the same identity.

## Compatibility

The default policy is `SQLSchemaCompatibilityBackward`:

- existing fields must keep the same type, nullability, enum values, and
  decimal metadata;
- removing a required field is rejected;
- newly added fields must be nullable.

`SQLSchemaCompatibilityForward` permits nullable field removal and rejects
removal of required fields. `SQLSchemaCompatibilityFull` applies both
directions. `SQLSchemaCompatibilityNone` records immutable versions without
comparing adjacent definitions. Re-registering identical metadata is
idempotent; reusing a version with different metadata is rejected.

Compatibility is deliberately conservative. It does not guess numeric
promotions, defaults, casts, or connector-specific migrations. Row-level
validation and quarantine remain available through the existing external
schema APIs.

## Bounds And Recovery

Zero options select bounded defaults: 1,024 sources, 256 versions per source,
and 256 columns per version. `Snapshot` returns owned definitions in
deterministic source order and preserves version registration order within a
source. `Restore` validates the complete replacement before publication, so a
bad snapshot leaves the current registry unchanged.

The registry stores schema metadata only. It does not retain rows, payloads,
credentials, or network clients. Source connectors own durable registry
storage and should persist a snapshot together with the ingestion checkpoint.

## Default And Tradeoff

`NewSQLSourceIngestionCoordinator()` remains registry-free. Existing callers
may omit `SchemaVersion` and keep the legacy path. The registry-enabled path
adds one bounded read-only map lookup per transaction. The paired benchmark
measured a median of 556.5 ns/op versus 472.1 ns/op for the legacy control,
with 449 B/op and 4 allocations/op for both. The clean pre-change control was
457.1 ns/op, 433 B/op, and 4 allocations/op. The safety gate is therefore
explicitly opt-in; callers should not configure it when they do not need
schema-version enforcement.

## Verification

```text
make test-mu040-schema-registry
make race-mu040-schema-registry
make vet-mu040-schema-registry
make benchmark-mu040-schema-registry
```

The full package command remains useful, but the current isolated base has
three unrelated pre-existing M-U05 arrangement-checkpoint failures:
`TestTypedTableAggregateArrangementCheckpointRestoresGlobalAggregate`,
`TestTypedTableAggregateArrangementCheckpointIsDeterministic`, and
`TestTypedTableAggregateArrangementCheckpointRestoresWithoutReplay`.
