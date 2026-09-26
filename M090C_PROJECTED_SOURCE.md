# M090c Projected Materialized Sources

M090c adds an optional ClickHouse-style projection pushdown hook for a
materialized SQL source. A compute node can ask a remote storage adapter for
only the fields required by a simple single-source query instead of fetching
complete row maps.

## Contract

Implement the optional interface next to the existing `SourceResolver`:

```go
type ProjectedSourceResolver interface {
	ResolveSQLProjectedSource(name, key string, fields []string) ([]Row, bool, error)
}

type ContextProjectedSourceResolver interface {
	ResolveSQLProjectedSourceContext(ctx context.Context, name, key string, fields []string) ([]Row, bool, error)
}
```

The context-aware interface is preferred when both are implemented. Return
`available=false` when the adapter cannot serve the request; SQL then uses the
existing full-row resolver. Return an error only when the adapter attempted
the request and it failed.

The `fields` slice contains canonical SQL field identifiers, without SQL
expressions or user-provided text. The resolver remains responsible for
authorization, snapshot consistency, and transport encoding.

## Automatic Scope

The executor automatically asks for a projection only when all of these are
true:

- the query has one `CACHE` or `KEYS` source;
- the `SELECT` list contains direct source fields;
- `WHERE` uses fields that can be represented by the existing predicate
  field extractor;
- there are no joins, CTEs, unions, grouping, aggregates, windows, ordering,
  distinctness, samples, `LIMIT BY`, `PREWHERE`, subqueries, or final sources.

The selected fields are deduplicated in select-then-predicate order. Complex
shapes, partition-aware resolvers, cached source materializations, legacy
resolvers, and adapters that return `available=false` retain the old path.
The same hook is used by the automatic native scalar dataflow path, so a
remote source is not fetched in full merely because native execution was
selected.

`CatalogResolver` and `SQLSession` forward the optional contract while
preserving their existing virtual-source and session-local precedence rules.
Existing resolver implementations remain source-compatible, and the feature
does not change the default wire or row serialization format.

## Example

```go
func (r RemoteSource) ResolveSQLProjectedSource(name, key string, fields []string) ([]hatSql.Row, bool, error) {
	if !r.SupportsProjection(name, key) {
		return nil, false, nil
	}
	return r.FetchRows(name, key, fields)
}
```

The remote implementation can encode `fields` in its own efficient request
format and construct rows containing only those fields. It must return a
consistent snapshot for the query just as the full-row resolver does.

## Tradeoff

The benchmark uses 4,096 rows with two required fields and three unused
payload fields. Projection reduced the measured source payload from 535,466
to 56,234 bytes per operation, about 9.5x lower. CPU improved by about 1.27x
in the regular executor and 1.19x in automatic native dataflow.

The current local benchmark's executor allocation cost is effectively flat:
the regular path changed from 4,104,078 to 4,104,255 B/op at the median and
from 20,512 to 20,516 allocations. The native path changed from 2,135,112 to
2,135,196 B/op and from 12,309 to 12,310 allocations. The byte reduction is
therefore a transport/source-materialization win; it is not a claim that the
local row-map executor heap is lower.

Raw samples and reproduction commands are in
[BENCHMARK.md#m090c-projected-materialized-sources](BENCHMARK.md#m090c-projected-materialized-sources).
