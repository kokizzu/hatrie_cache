# Transparent SQL Projections

`MaterializedViews` can optionally act as a projection catalog for exact
read-only queries. This is an opt-in execution shortcut; the default SQL path
does not inspect or use the catalog.

## Use

Create and refresh a named materialized view as usual, then pass the registry
through `QueryOptions`:

```go
views := hatSql.NewMaterializedViews()
_, err := views.Create(ctx, hatSql.MaterializedViewDefinition{
    Name:         "event_names",
    Query:        "FROM CACHE('events') SELECT name",
    Dependencies: []string{"events"},
}, resolver, hatSql.QueryOptions{})
if err != nil {
    return err
}

result, err := hatSql.ExecuteSQLQueryContext(ctx,
    "FROM CACHE('events') SELECT name",
    resolver,
    hatSql.QueryOptions{ProjectionCatalog: views},
)
```

An eligible result includes a `PROJECTION HIT` plan step. The returned columns,
rows, and plan are cloned, so callers cannot mutate the retained snapshot.

## Eligibility And Freshness

A projection is selected only when all of these conditions hold:

- The trimmed query text exactly matches a registered view query.
- The resolver implements `SourceVersionResolver`.
- Every declared dependency has a non-empty current `CACHE` source version.
- Every current dependency version equals the version captured at create or
  refresh time.
- The requested collation matches the collation used to create or refresh the
  view, and no `IndexHint` is active.

Resolvers without source versions always use the ordinary SQL executor. A
source mutation must advance its version; otherwise no cache or projection can
prove freshness. `MaterializedViews.Create` and `RefreshChanged` bypass the
catalog while building a snapshot, preventing recursive reuse.

`MaxRows` and `MaxResultBytes` are still enforced on a projection hit. Context
cancellation is checked before returning. Queries with other budgets retain
the established executor when they need detailed operator accounting.

## Boundaries

This feature does not rewrite arbitrary SQL into a typed aggregate or infer
dependencies from query text. It intentionally avoids NULL, alias, collation,
ordering, and source-version mismatches. It complements explicit incremental
projections and typed arrangements rather than replacing them.

## Verification

```text
make test-sql-projection-selection
make benchmark-sql-projection-selection
```

The recorded benchmark is an in-process four-row source comparison. A fresh
Across five benchmark samples, the median exact projection hit was about 2.25x
faster, used about 54.5% less allocated heap, and made about 62% fewer
allocations than executing the same query directly. The hit still clones the
result for isolation, so the benchmark does not claim zero-copy output.
