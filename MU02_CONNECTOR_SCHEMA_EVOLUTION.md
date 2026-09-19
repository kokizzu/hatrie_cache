# M-U02 Connector Schema Evolution

`hatSql.PlanCatalogSchemaEvolution` adds an explicit, deterministic compatibility
boundary for SQL catalog changes. It is intended for connector or ingestion
code that receives an upstream schema version and needs to decide whether that
version can be exposed to existing readers.

## What It Does

- Normalizes source, field, and index order before comparing catalogs.
- Produces SHA-256 fingerprints and a deterministic list of changes.
- Accepts nullable field additions by default.
- Rejects required field additions, source-kind changes, narrowing type changes,
  and nullable-to-required changes.
- Rejects removal of a field while a retained index still depends on it.
- Allows destructive field/source removal only with `AllowFieldRemoval: true`.
- Allows only exact integer-to-number widening with
  `AllowNumericWidening: true`; values that would lose `float64` precision are
  rejected instead of silently changing.
- Projects rows across a compatible mixed-version boundary without mutating the
  input rows.
- Publishes a validated catalog through `CatalogSchemaRegistry.Replace` under
  one mutex-protected generation boundary, and supports compare-and-swap style
  rollback with `Rollback(expectedGeneration, ...)`.

The default policy is conservative. Omitting options does not silently discard
fields or change typed values.

## Example

```go
before := hatSql.Catalog{
    Namespaces: []string{"default"},
    Sources: []hatSql.CatalogSource{{
        Namespace: "default",
        Name:      "orders",
        Kind:      "table",
        Fields: []hatSql.CatalogField{
            {Name: "id", Type: "int64"},
            {Name: "total", Type: "number", Nullable: true},
        },
    }},
}
after := before
after.Sources[0].Fields = append(after.Sources[0].Fields,
    hatSql.CatalogField{Name: "currency", Type: "string", Nullable: true},
)

plan, err := hatSql.PlanCatalogSchemaEvolution(
    before,
    after,
    hatSql.CatalogSchemaEvolutionOptions{},
)
if err != nil {
    return err
}

rows, err := plan.ProjectRows("default", "orders", []hatSql.Row{
    {"id": int64(7), "total": 12.50},
})
// rows is: {"id": int64(7), "total": 12.50, "currency": nil}
```

For a shared catalog boundary, initialize a registry once and replace only
with a plan that passes policy checks:

```go
registry, err := hatSql.NewCatalogSchemaRegistry(before)
if err != nil {
    return err
}
plan, generation, err := registry.Replace(after,
    hatSql.CatalogSchemaEvolutionOptions{})
if err != nil {
    return err
}
_ = plan
// A reader that observed generation 2 can roll back only if it is still current.
nextGeneration, err := registry.Rollback(generation, before,
    hatSql.CatalogSchemaEvolutionOptions{AllowFieldRemoval: true})
```

`CatalogSchemaRegistry.Resolver` captures an independent catalog snapshot. A
caller that needs mixed-version ingestion should retain the returned evolution
plan and call `ProjectRows` while the connector changes its upstream schema.
The registry does not perform connector I/O, persist the catalog, or infer
dependencies for views, sinks, or external systems; those remain caller-owned.

## Compatibility Rules

| Change | Default | Opt-in | Notes |
| --- | --- | --- | --- |
| Add nullable field | Allowed | None | Missing values project as `nil`. |
| Add required field | Rejected | None | Existing rows cannot supply a value. |
| Remove field/source | Rejected | `AllowFieldRemoval` | A retained index dependency still rejects the plan. |
| Integer to number/float64/double | Rejected | `AllowNumericWidening` | Exact values only; lossy values fail projection. |
| Number to integer or other narrowing | Rejected | None | No implicit coercion. |
| Required to nullable | Allowed | None | Existing values remain valid. |
| Nullable to required | Rejected | None | Existing `nil` values would be invalid. |
| Source kind change | Rejected | None | Requires a deliberate new source identity. |
| Add/remove index | Allowed | None | Every target index must reference existing fields. |

## Verification

The focused tests cover nullable additions, row projection, dependency
rejection, required-field rejection, deterministic fingerprints, numeric
widening, lossy-value rejection, atomic failed replacement, and generation
checked rollback:

```text
make test-m052
ok   hatrie_cache/hat/hatSql  0.006s
```

## Benchmark

The control benchmark uses 64 sources with four fields and repeatedly resolves
`information_schema.fields`. The feature benchmarks use the same 64-source
catalog, add one nullable field to every source, project 128 rows, or replace
the registry. Each result is the raw output of five runs on an AMD Ryzen 9
5950X, Go's `-benchmem`, and the repository's `make` targets.

### Control: clean origin versus feature branch

| Benchmark | Clean origin samples (ns/op) | Feature samples (ns/op) | Median feature | B/op | allocs/op | Observed ratio |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Resolver control | 108939, 102917, 104912, 101128, 101907 | 89441, 85477, 86007, 86719, 90054 | 86719 | 106816 | 1542 | 1.19x lower latency |

The resolver path's allocation and memory figures are identical. The lower
control median is an observed run result, not a claimed feature speedup: the
new public code is not called by this control path, so it should be treated as
normal benchmark variance. The important result is no measured hot-path
allocation or memory regression.

### New schema-evolution paths

| Benchmark | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `BenchmarkCatalogSchemaEvolutionPlan` | 454355 | 258120 | 1999 |
| `BenchmarkCatalogSchemaEvolutionProjectRows` | 262619 | 129024 | 1185 |
| `BenchmarkCatalogSchemaEvolutionRegistryReplace` | 546499 | 324456 | 2594 |

Raw feature output:

```text
BenchmarkCatalogSchemaEvolutionControl-32 13524 89441 ns/op 106816 B/op 1542 allocs/op
BenchmarkCatalogSchemaEvolutionControl-32 13046 85477 ns/op 106816 B/op 1542 allocs/op
BenchmarkCatalogSchemaEvolutionControl-32 13651 86007 ns/op 106816 B/op 1542 allocs/op
BenchmarkCatalogSchemaEvolutionControl-32 13502 86719 ns/op 106816 B/op 1542 allocs/op
BenchmarkCatalogSchemaEvolutionControl-32 14343 90054 ns/op 106816 B/op 1542 allocs/op
BenchmarkCatalogSchemaEvolutionPlan-32 2355 456435 ns/op 258120 B/op 1999 allocs/op
BenchmarkCatalogSchemaEvolutionPlan-32 2524 454355 ns/op 258120 B/op 1999 allocs/op
BenchmarkCatalogSchemaEvolutionPlan-32 2653 447782 ns/op 258120 B/op 1999 allocs/op
BenchmarkCatalogSchemaEvolutionPlan-32 2493 459032 ns/op 258120 B/op 1999 allocs/op
BenchmarkCatalogSchemaEvolutionPlan-32 2690 449206 ns/op 258120 B/op 1999 allocs/op
BenchmarkCatalogSchemaEvolutionProjectRows-32 5056 259966 ns/op 129024 B/op 1185 allocs/op
BenchmarkCatalogSchemaEvolutionProjectRows-32 4324 267857 ns/op 129024 B/op 1185 allocs/op
BenchmarkCatalogSchemaEvolutionProjectRows-32 4632 254470 ns/op 129024 B/op 1185 allocs/op
BenchmarkCatalogSchemaEvolutionProjectRows-32 4461 270664 ns/op 129024 B/op 1185 allocs/op
BenchmarkCatalogSchemaEvolutionProjectRows-32 3877 262619 ns/op 129024 B/op 1185 allocs/op
BenchmarkCatalogSchemaEvolutionRegistryReplace-32 2342 546499 ns/op 324456 B/op 2594 allocs/op
BenchmarkCatalogSchemaEvolutionRegistryReplace-32 2106 547033 ns/op 324456 B/op 2594 allocs/op
BenchmarkCatalogSchemaEvolutionRegistryReplace-32 2085 542937 ns/op 324456 B/op 2594 allocs/op
BenchmarkCatalogSchemaEvolutionRegistryReplace-32 2206 532405 ns/op 324456 B/op 2594 allocs/op
BenchmarkCatalogSchemaEvolutionRegistryReplace-32 1981 548266 ns/op 324457 B/op 2594 allocs/op
```

The control path is unchanged. Planning and replacement are deliberately
control-plane operations that copy and validate the catalog; they are not on
the row lookup path. Projection allocates one output map per row by design so
the input remains immutable.
