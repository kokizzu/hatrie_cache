# M-U14 Dependency Catalog

M-U14 adds a versioned object and direct-dependency view to the SQL catalog.
It is inspired by Materialize's catalog-oriented view of dataflow objects: the
catalog can describe sources, views, indexes, and sinks in one snapshot and
show how one object depends on another.

## API

`hatSql.Catalog` remains backward compatible for existing namespace, source,
and index declarations. The new fields are:

```go
Catalog{
    Version: 12,
    Objects: []CatalogObject{
        {
            Namespace: "public",
            Name:      "recent_orders",
            Kind:      CatalogObjectKindView,
            Type:      "materialized",
            Version:   4,
            State:     "ready",
        },
        {
            Namespace: "public",
            Name:      "orders_sink",
            Kind:      CatalogObjectKindSink,
            Type:      "kafka",
            Version:   2,
            State:     "ready",
        },
    },
    Dependencies: []CatalogDependency{
        {
            Namespace:          "public",
            Object:             "recent_orders",
            ObjectKind:         CatalogObjectKindView,
            DependsOnNamespace: "public",
            DependsOn:          "orders",
            DependsOnKind:      CatalogObjectKindSource,
        },
        {
            Namespace:          "public",
            Object:             "orders_sink",
            ObjectKind:         CatalogObjectKindSink,
            DependsOnNamespace: "public",
            DependsOn:          "recent_orders",
            DependsOnKind:      CatalogObjectKindView,
        },
    },
}
```

`Catalog.Version` identifies the metadata snapshot. A zero value is treated as
version `1` so existing callers get a stable version without changing their
initialization. An object with `Version == 0` inherits the catalog version;
an empty object state defaults to `ready`.

Existing `Sources` and `Indexes` automatically appear as `source` and `index`
objects. An index also automatically contributes an `index -> source`
dependency. Explicitly publishing the same edge is safe and is deduplicated.
View and sink objects are caller-owned metadata: the SQL engine does not guess
them from unrelated registries.

## SQL Relations

`information_schema.objects` has this stable schema:

| Column | Meaning |
| --- | --- |
| `catalog_version` | Version of the catalog snapshot. |
| `namespace` | Object namespace. |
| `name` | Object name. |
| `kind` | `source`, `view`, `index`, or `sink` (or a caller-defined kind). |
| `type` | Backend or object subtype, such as `CACHE`, `hash`, or `kafka`. |
| `object_version` | Object definition version, or the catalog version when omitted. |
| `state` | Caller-published state; defaults to `ready`. |

`information_schema.dependencies` contains direct edges:

| Column | Meaning |
| --- | --- |
| `catalog_version` | Version of the catalog snapshot. |
| `namespace` / `object` / `object_kind` | The dependent object. |
| `depends_on_namespace` / `depends_on` / `depends_on_kind` | The object it directly reads or uses. |
| `ordinal_position` | Stable edge position among the same dependent object. |

Both relations are read-only virtual sources owned by `CatalogResolver`.
Rows are normalized and sorted deterministically. Malformed explicit object or
dependency metadata returns an error rather than exposing a partial row.

The shorthand commands are:

```text
SHOW OBJECTS
SHOW DEPENDENCIES
```

They compile to the corresponding information-schema queries and can be used
through the same `ExecuteSQLQueryParameters` path as `SHOW SOURCES`.

## Bounded Traversal

For programmatic impact analysis, use:

```go
paths, err := catalog.DependencyClosure(
    CatalogObjectRef{
        Namespace: "public",
        Name:      "orders_sink",
        Kind:      CatalogObjectKindSink,
    },
    CatalogDependencyTraversalOptions{},
)
```

The result is deterministic breadth-first order. A cycle is returned as an
edge but visited objects are never queued twice, so cycles cannot cause an
unbounded walk. Zero limits use `DefaultCatalogDependencyMaxDepth` (`64`) and
`DefaultCatalogDependencyMaxRows` (`65,536`). Negative limits are invalid.
If either limit would make the result incomplete, the method returns
`ErrCatalogDependencyDepthExceeded` or `ErrCatalogDependencyLimitExceeded`
instead of silently truncating the result. `ErrCatalogDependencyInvalid`
covers malformed roots and dependency metadata.

The closure accepts dependencies whose target is outside the current catalog;
such an edge is reported but cannot be traversed further. This supports a
partial snapshot without inventing object metadata.

## Benchmark

The benchmark runs five samples on an AMD Ryzen 9 5950X through
`make benchmark-mu014-object-dependency-catalog`:

| Workload | Median ns/op | B/op | Allocs/op | Notes |
| --- | ---: | ---: | ---: | --- |
| Pre-change one-row `information_schema.sources` | 8,083 | 6,384 | 36 | Baseline captured before M-U14. |
| Post-change one-row `information_schema.sources` | 7,930 | 6,432 | 36 | No regression observed; normal benchmark noise applies. |
| `information_schema.objects` | 181,282 | 146,314 | 999 | 80 objects, including 32 sources, 32 indexes, and 16 views. |
| `DependencyClosure` | 19,594 | 19,064 | 58 | Three reachable edges. |

The object query is intentionally a metadata snapshot and is not on the
ordinary source read path. Its cost scales with the number of catalog rows and
the SQL projection/sort requested. The old source path remains available, and
callers that do not publish `Objects` or `Dependencies` pay no cost unless they
query the new relations. The traversal bounds prevent an accidental large or
cyclic graph from consuming unbounded CPU or memory.

Raw output is retained in [BENCHMARK.md](BENCHMARK.md#mu-014-object-dependency-catalog).
