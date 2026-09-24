# M-U39 Partition and Order Declarations

M-U39 adds an opt-in metadata contract for SQL sources that are physically
partitioned and maintain a deterministic order inside each partition. The
declaration is visible to `EXPLAIN` and can be shared with a source adapter
when it decides whether a literal predicate is safe to offer to the existing
partition-pruning resolver.

## Registration

Use `CACHE` (or the source kind used by the resolver) as `Source` and the
existing source key as `Key`:

```go
registry := hatSql.NewSQLPartitionOrderRegistry(0) // 0 selects the bounded default
err := registry.Register(hatSql.SQLPartitionOrderDeclaration{
    Source:          "CACHE",
    Key:             "events",
    PartitionFields: []string{"region"},
    OrderFields: []hatSql.SQLPartitionOrderField{
        {Field: "event_time", Desc: true, NullsLast: true},
        {Field: "id"},
    },
})
```

Registration validates and copies all fields. The registry is concurrency-safe,
bounded to 256 declarations by default, and replacing an existing `(Source,
Key)` does not consume another slot. A caller can pass a positive capacity to
`NewSQLPartitionOrderRegistry` when a different bound is appropriate.

Attach the registry to one query through `SQLQueryOptions`:

```go
result, err := hatSql.ExecuteSQLQueryParameters(
    ctx,
    "EXPLAIN FROM CACHE('events') SELECT region, event_time",
    resolver,
    nil,
    hatSql.SQLQueryOptions{PartitionOrderResolver: registry},
)
```

The scan plan includes a structured `partition_order` value and a concise
detail such as:

```text
CACHE("events"); PARTITION BY region ORDER BY event_time DESC NULLS LAST, id ASC
```

`EXPLAIN PIPELINE` uses the same metadata. A nil resolver is the default and
keeps the legacy plan shape.

## Filter pushdown

`SQLPartitionOrderDeclaration.SupportsPartitionPredicate` returns true only
for a partition field and a literal `=`, `IN`, `<`, `<=`, `>`, or `>=`
predicate. It is an eligibility check, not a pruning operation:

```go
func resolvePrunedSource(
    registry *hatSql.SQLPartitionOrderRegistry,
    resolver hatSql.PartitionPruningSourceResolver,
    predicate hatSql.SQLPartitionPredicate,
) ([]hatSql.SQLSourcePartition, bool, error) {
    declaration, available, err := registry.ResolveSQLPartitionOrder("CACHE", "events")
    if err == nil && available && declaration.SupportsPartitionPredicate(predicate) {
        return resolver.ResolveSQLSourcePartitionsForPredicate(
            "CACHE", "events", predicate,
        )
    }
    return nil, false, err
}
```

The source adapter remains responsible for proving that no matching partition
is omitted. The SQL executor still evaluates the original predicate after the
source is resolved, so stale or missing declarations fall back safely to the
ordinary source path and cannot change query semantics.

## Scope and tradeoff

The declaration stores only source/key metadata and field names; it does not
rewrite rows, build an index, start a worker, or retain source data. Lookup
allocates a copy for isolation because callers may mutate their returned
metadata. The benchmark measures this control-plane cost separately from
ordinary query execution. This feature is useful when a connector already has
partition and order knowledge; it does not invent physical partitioning for a
plain resolver.
