# CH-U18 Composite Primary-Mark Pruning

Hatrie SQL now supports an optional lexicographically ordered tuple of numeric
fields for columnar sparse-primary pruning. This is useful for data ordered by
a leading partition key and then by a local key, such as `(tenant, id)`.

## Configuration

The feature is opt-in and remains off by default:

```go
ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
	Enabled:            true,
	SparsePrimaryIndex: true,
	SparsePrimaryFields: []string{
		"tenant",
		"id",
	},
}
```

`SparsePrimaryFields` accepts up to eight distinct numeric fields. Field order
is significant. The existing `SparsePrimaryField` option and behavior remain
available for a single ordered numeric field.

## Behavior

The cache records the first and last tuple of each segment in flat,
segment-major arrays. A query can use the metadata when it has equality
predicates for a leading prefix and an equality or range predicate at the next
field. Predicates after the first ranged field are evaluated normally but do
not make the tuple interval non-contiguous.

The builder publishes composite marks only when every configured field is
numeric, present for every row, non-NaN, and nondecreasing under lexicographic
comparison. Incomplete metadata, NULL/missing values, NaN values, or a tuple
ordering violation fall back to the existing scan and single-field metadata;
they never produce an unsafe skip.

There is no wire or persistence-format change. The tuple arrays are in-memory
columnar-cache metadata and are rebuilt or discarded with that cache.

## Tradeoffs

Composite range planning performs more numeric comparisons than a single-field
mark lookup, but can avoid scanning the rest of a leading-key group. The
benchmark uses 100 tenants, 8,192 ordered rows per tenant, 256 rows per
segment, and a query for `tenant = 42 AND id >= 7000`. It reports planner
nanoseconds, allocations, and retained segments together so selectivity is
visible alongside CPU cost. See [BENCHMARK.md](BENCHMARK.md#ch-u18-composite-primary-mark-pruning).
