# FINAL Schema Registry

The FINAL schema registry is an opt-in adapter for ClickHouse-style FINAL
metadata. It converts named key/version/sign fields into the existing
`SQLFinalOptions` callback contract. It does not change normal queries and it
does not enable FINAL automatically.

## Usage

```go
registry, err := hatSql.NewSQLFinalSchemaRegistry(hatSql.SQLFinalSchemaRegistryOptions{})
if err != nil {
	return err
}
if err := registry.Upsert("cache", "events", hatSql.SQLFinalSchemaDefinition{
	Mode:         hatSql.SQLFinalReplacing,
	KeyFields:    []string{"tenant_id", "id"},
	VersionField: "version",
}); err != nil {
	return err
}

result, err := hatSql.ExecuteSQLQueryContext(
	ctx,
	query,
	sourceResolver,
	hatSql.SQLQueryOptions{FinalSourceOptions: registry.Resolver()},
)
```

Use `SQLFinalCollapsing` with `SignField` instead of `VersionField` when rows
carry `+1` and `-1` signs. Replacing definitions require exactly one version
field; collapsing definitions require exactly one sign field. Key fields are
combined in declaration order with collision-safe typed encoding.

Source kinds are normalized to uppercase because SQL parsing canonicalizes
them (`cache` and `CACHE` match). Source keys remain case-sensitive. Registry
updates, deletes, resolves, and snapshots are safe for concurrent callers.

The registry is bounded. A zero limit selects 256 definitions; the accepted
maximum is 65,536. Updating an existing `(kind, key)` does not consume another
slot. `Snapshot` returns detached, sorted metadata.

Missing or unsupported version/sign values return `ErrSQLFinalSchemaRowValue`.
Supported numeric values include the Go signed and unsigned integer types,
exact integral floats, and decimal strings/byte slices. Keys support common
scalar values, `time.Time`, byte slices, and a deterministic formatted fallback
for other values.

## Benchmark

Measured on AMD Ryzen 9 5950X, Linux/amd64, with 256 input rows and Go
`testing.B`. The registry benchmark includes a source lookup on every outer
iteration, matching the query contract; the direct baseline uses hand-written
callbacks.

| Workload | Path | ns/op | B/op | allocs/op | Registry / direct CPU | Registry / direct memory | Registry / direct allocs |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| One string key | Direct callback | 45,908 | 62,120 | 262 | 1.00x | 1.00x | 1.00x |
| One string key | Registry | 44,552 | 62,128 | 263 | 0.97x | 1.00x | 1.00x |
| Two string keys | Direct callback | 57,829 | 66,664 | 518 | 1.00x | 1.00x | 1.00x |
| Two string keys | Registry | 65,027 | 70,320 | 519 | 1.12x | 1.05x | 1.00x |

The single-key difference is within normal benchmark noise and is effectively
neutral. Composite keys pay for generic length-delimited encoding: about 12%
CPU and 5% retained operation bytes in this run, with no meaningful allocation
count change. Latency-sensitive callers can keep using a direct
`SQLFinalOptions` callback for that case. Callers that do not attach
`registry.Resolver()` pay no registry cost.

Run the measurements with:

```text
make benchmark-ch004-final-schema-registry
```
