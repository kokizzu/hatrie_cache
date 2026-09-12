# Named Space Catalog

`hat/hatSchema.SpaceCatalog` is an optional Tarantool-style metadata catalog.
Each entry combines a named versioned source, its constraints, and the indexes
that an execution layer may maintain or select.

## Define and query

```go
catalog, err := hatSchema.NewSpaceCatalog([]hatSchema.SpaceDefinition{
    {
        Name:    "orders",
        Version: 7,
        Source: hatSchema.Source{
            Name: "orders",
            Columns: []hatSchema.Column{
                {Name: "id", Type: hatSchema.TypeText},
                {Name: "region", Type: hatSchema.TypeText},
            },
        },
        Indexes: []hatSchema.IndexDefinition{
            {
                Name:    "primary",
                Kind:    hatSchema.IndexKindTree,
                Columns: []string{"id"},
                Unique:  true,
            },
        },
    },
})
if err != nil {
    return err
}

orders, ok := catalog.Lookup("orders")
allSpaces := catalog.List()
```

`Lookup` and `List` return deep copies, so a caller cannot mutate catalog state
through a returned source, constraint, or index slice. `Upsert` validates and
publishes one complete definition atomically. `Delete` removes one definition.
Names are trimmed and listing is sorted by normalized name.

## Definition fields

| Field | Meaning |
| --- | --- |
| `Name` | Stable named-space identifier. It must match `Source.Name` when the source name is supplied. |
| `Version` | Caller-owned schema version for migration and compatibility checks. |
| `Source` | Existing ordered columns and constraints from `hatSchema`. |
| `Indexes` | Named hash, tree, R-tree, or functional index declarations. Functional indexes require an expression. |

The catalog validates non-empty unique column, constraint, index, and index
column names, rejects unknown index columns, and bounds one catalog at 4,096
spaces and one space at 256 indexes. It stores declarations only; it does not
build an index or change the existing SQL planner.

## Benchmark

Command:

```text
make benchmark-t-u15
```

Five local runs on the AMD Ryzen 9 5950X environment with 64 definitions:

| Operation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Lookup one cloned definition | 161.7 | 176 | 3 |
| List all definitions, sorted | 16,922 | 19,640 | 196 |

The catalog is opt-in. Existing schema validation and SQL execution do not
pay these lookup/list costs unless an integration chooses to consult the
catalog.
