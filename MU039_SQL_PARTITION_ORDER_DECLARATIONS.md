# SQL Partition And Order Declarations

`hatSql.SQLPartitionDeclaration` lets a source publish the physical layout
that its resolver already maintains: partition-key fields, ordered fields,
direction, NULL placement, and an optional partition count.

The declaration is advisory metadata. It does not rewrite rows, change result
semantics, or infer partition bounds. Existing
`PartitionedSourceResolver`, `PartitionPruningSourceResolver`, and
`PartitionedOrderedSourceResolver` implementations still own actual reads and
must preserve complete SQL results.

## Register Metadata

```go
resolver := hatSql.CatalogResolver{
    Source: applicationSource,
    Catalog: hatSql.Catalog{
        Partitions: []hatSql.SQLPartitionDeclaration{
            {
                Namespace:      "default",
                Source:         "orders",
                Kind:           "CACHE",
                PartitionBy:    []string{"region"},
                OrderBy:        []hatSql.SQLPartitionOrder{{Field: "created_at", Desc: true}},
                PartitionCount: 3,
            },
        },
    },
}
```

Resolvers with dynamic metadata can implement
`SQLPartitionDeclarationResolver` instead. `CatalogResolver` checks its
catalog first and then forwards to the application resolver. `SQLSession`
forwards the same contract while session-local temporary tables, results, and
views retain precedence.

Invalid declarations are rejected by `ValidateSQLPartitionDeclaration` and by
catalog/resolver publication. Field counts and text lengths are bounded, and
duplicates within either list are rejected; a field may legitimately appear
once in each list.

## Inspect Metadata

Use the SQL catalog shortcut:

```sql
SHOW PARTITIONS
```

The equivalent source is
`information_schema.partitions`. It returns one row per `PARTITION BY` or
`ORDER BY` field with `partition_count`, `role`, `field`,
`ordinal_position`, `descending`, and `nulls_first`.

`EXPLAIN` returns a parallel `QueryResult.Partitioning` annotation list. Each
annotation contains the zero-based plan `StepIndex` and the declaration for
that source step. This keeps the existing `ExplainStep` representation and its
default allocation size unchanged.

## Cost And Defaults

Declarations are opt-in. Ordinary query execution does not consult or retain
them, and an ordinary explain without a declaration retains the baseline
allocation count. The raw before/after measurements are in
[`BENCHMARK.md`](BENCHMARK.md#m-u39-sql-partition-and-order-declarations).

The declaration-bearing explain path adds metadata allocations because it
copies the bounded declaration into the result. That cost is paid only when a
caller publishes a declaration and asks for `EXPLAIN`; it does not replace the
source resolver's pruning or ordered-read implementation.
