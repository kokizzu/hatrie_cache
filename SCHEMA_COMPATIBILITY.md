# Rolling Schema Compatibility

`hatSchema.CheckRollingCompatibility` is a deployment preflight for comparing
the schema currently used by replicas with a proposed next version.

```go
report, err := hatSchema.CheckRollingCompatibility(previous, next)
if err != nil {
    return err // one of the schemas is invalid
}
if !report.Compatible {
    return fmt.Errorf("schema rollout rejected: %#v", report.Changes)
}
```

## Conservative Rules

The checker returns `Compatible: true` only for changes that preserve the old
and new node view during a rolling deployment:

- advancing the schema version without changing fields;
- appending nullable columns; and
- relaxing an existing `NOT NULL` declaration.

It rejects source additions/removals, column removal or reordering, type
changes, required-column additions, making a column required, constraint
changes, and version regression. The returned `Changes` list is deterministic
and includes a machine-readable `Kind`, source, column, and optional detail.

The comparison validates both schemas and never mutates either input. A
cloned schema with nil versus empty constraint slices is treated as unchanged.

## Replication Boundary

This is a preflight/reporting API. The existing opt-in
`RequireReplicationSchemaCompatibility` path still requires an exact version
and fingerprint on every replicated command. No receiver silently accepts a
new schema based only on this report; relaxing the wire gate requires a
separate protocol contract with explicit schema registration and rollout
state. Legacy replication remains unchanged.

For an explicit rolling transition, construct a
`ReplicationSchemaCompatibilityPolicy` with the current schema and each
validated previous schema, then pass the same policy together with the current
`ReplicationSchemaContract` to `MonitoringOptions` or `CacheGRPCOptions`:

```go
policy, err := hatCache.NewReplicationSchemaCompatibilityPolicy(current, previous)
if err != nil {
    return err
}
options := hatCache.MonitoringOptions{
    ReplicationSchema:                     hatCache.NewReplicationSchemaContract(current),
    SchemaCompatibilityPolicy:             policy,
    RequireReplicationSchemaCompatibility: true,
}
```

The policy accepts only the current contract or exact contracts derived from
the supplied history. It rejects unknown fingerprints and rejects a policy
whose current contract differs from the server's configured current contract.
The policy is opt-in; a nil policy retains exact matching. Batch metadata is
inherited by child replication commands on both transports.

## Performance

The small two-column fixture measured a median `694.6 ns/op`, `224 B/op`, and
`3 allocs/op` across five local runs. The exact contract lookup in an enabled
replication command costs `24.55 ns/op`, `0 B/op`, and `0 allocs/op` in the
same fixture. See
[`BENCHMARK.md`](BENCHMARK.md#rolling-schema-compatibility) for raw results.
