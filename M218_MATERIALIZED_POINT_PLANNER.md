# M218: Materialized Point Planner

M218 lets `QueryOptions.ProjectionCatalog` plan a narrow filtered query against
a maintained materialized view. The planner compares a point posting with a
snapshot arrangement scan, then falls back to the ordinary source executor when
the snapshot cannot prove correctness.

## Supported Shape

The materialized view and request must have the same simple `CACHE` source and
the same projection. The request may add one binary-collation equality
predicate on a directly projected field:

```go
definition := hatSql.MaterializedViewDefinition{
    Name:              "people_projection",
    Query:             "FROM CACHE('people') AS p SELECT p.id, p.region, p.name",
    Dependencies:      []string{"people"},
    PointLookupFields: []string{"region"},
}
views.Create(ctx, definition, resolver, hatSql.QueryOptions{})

result, err := hatSql.ExecuteSQLQueryContext(ctx,
    "FROM CACHE('people') AS p WHERE p.region = 'sg' SELECT p.id, p.region, p.name",
    resolver,
    hatSql.QueryOptions{ProjectionCatalog: views},
)
```

Joins, grouping, ordering, limits, unions, prewhere, non-binary collations,
and projection mismatches retain the normal executor. A source version
resolver is required, and stale snapshots are never selected.

## Selection Rule

- A configured point posting is selected when its candidate count is at most
  half of the maintained snapshot row count.
- A dense posting, an unconfigured field, or an unsupported posting value uses
  `MATERIALIZED ARRANGEMENT SCAN` over the retained snapshot.
- Missing freshness proof or an unsupported query shape falls through to the
  ordinary source/index planner.

Both result paths clone returned rows. Refresh publishes the snapshot and
postings atomically, so readers cannot observe mixed revisions.

## Tradeoff

The planner adds a parsed base-query pointer per materialized view and compares
the source version before selecting a snapshot. Point postings still carry the
M217 refresh/storage overhead. The 50% cutoff avoids using postings when a
linear snapshot scan is expected to touch most rows.

See [BENCHMARK.md](BENCHMARK.md#m218-materialized-point-planner) for the
before/after measurements.
