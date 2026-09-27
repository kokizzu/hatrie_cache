# ClickHouse-style `FINAL` Schema Store

`SQLFinalSchemaRegistry` turns source metadata into the callbacks used by
`FINAL` reconciliation. The registry is still opt-in. This extension makes
those contracts restart-safe without putting filesystem work on normal query
execution.

## Usage

```go
store, err := hatSql.NewFileSQLFinalSchemaStore("/var/lib/hatrie/final-schema.hfs")
if err != nil {
    return err
}

registry, err := hatSql.NewSQLFinalSchemaRegistryFromStore(
    context.Background(),
    store,
    hatSql.SQLFinalSchemaRegistryOptions{},
)
if err != nil {
    return err
}

if err := registry.Upsert("table", "orders", hatSql.SQLFinalSchemaDefinition{
    Mode:         hatSql.SQLFinalReplacing,
    KeyFields:    []string{"tenant", "id"},
    VersionField: "version",
}); err != nil {
    return err
}

queryOptions.FinalSourceOptions = registry.Resolver()
if err := registry.SaveToSQLFinalSchemaStore(context.Background(), store); err != nil {
    return err
}
```

`NewSQLFinalSchemaRegistryFromStore` treats a missing file as an empty
registry. `SaveToSQLFinalSchemaStore` is explicit: callers decide when a
metadata change is durable, so `Upsert` and `Delete` remain allocation- and
filesystem-free. Source kinds are restored in the registry's canonical
uppercase form.

## Format and Safety

`FileSQLFinalSchemaStore` writes a deterministic `HFS1` binary snapshot:

- registrations are sorted by source kind and key;
- key fields, version fields, and sign fields are length-delimited;
- the payload is checked with CRC32C before any registration is exposed;
- definition count and total bytes are bounded (`256` and `1 MiB` by default);
- saves write a `0600` temporary file, `Sync`, and atomically rename it;
- malformed, duplicated, oversized, or invalid definitions are rejected;
- a failed validation never replaces the previous snapshot.

Limits can be changed with `SQLFinalSchemaStoreOptions`, within the package's
hard bounds. The store persists metadata only; materialized rows are rebuilt
against current source versions after restore.

## Measured Tradeoff

AMD Ryzen 9 5950X, Linux, Go benchmark harness. The in-memory baseline was
measured before the store implementation. File benchmarks used five samples
and `-benchtime=100x`; save includes temporary-file creation, `Sync`, and
atomic rename.

| Operation | Median ns/op | Bytes/op | Allocs/op | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Existing in-memory `Snapshot` (2 contracts) | 374 | 408 | 6 | unchanged hot-path baseline |
| Durable file save (2 contracts) | 796,305 | 2,108 | 24 | explicit durability cost, about 2,100x slower than memory snapshot |
| Durable file load (2 contracts) | 9,690 | 1,384 | 32 | startup/reload cost, not query-path cost |

The persistence path is therefore a durability feature, not a speed
optimization. Keeping it explicit avoids imposing the measured save cost on
every schema mutation or query.

## Verification

```text
make test-ch004-final-schema-store
make test-ch004-final-schema-store-package
make race-ch004-final-schema-store
make vet-ch004-final-schema-store
make benchmark-ch004-final-schema-store
```
