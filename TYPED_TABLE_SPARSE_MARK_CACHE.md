# Typed-table Sparse-primary Mark Cache

Typed tables can optionally retain a compact LRU of sparse-primary range
marks separately from full columnar data layouts. This is inspired by
ClickHouse's separation of sparse marks from data-part caching: a full batch
can be evicted while ordered range metadata remains available for selective
reads.

## Configuration

Both the columnar cache and the sparse-primary index must be enabled:

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
	Name:    "events",
	Columns: []hatSql.TypedTableColumn{{Name: "id", Kind: hatSql.TypedTableInt64}},
	ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
		Enabled:                   true,
		MaxBytes:                  1 << 20,
		MinReads:                  2,
		RowsPerSegment:            256,
		SparsePrimaryIndex:        true,
		SparsePrimaryField:        "id",
		SparsePrimaryMarkCache:    true,
		SparsePrimaryMarkMaxBytes: 1 << 20,
	},
})
if err != nil {
	return err
}
```

`SparsePrimaryMarkCache` is false by default. When enabled,
`SparsePrimaryMarkMaxBytes <= 0` selects a 1 MiB limit. The mark cache retains
only the configured primary field's numeric segment bounds, uses LRU eviction,
and does not retain the full `ColumnarBatch` or unrelated sidecars. When
`SparsePrimaryFields` configures a composite numeric key, the same cache also
retains the flat first/last tuple arrays needed for lexicographic pruning.
Tuple metadata and the retained leading-field bounds share the same byte
budget.

When a full layout is available, the existing zero-copy borrowed batch path is
used. When the full layout is absent but a mark is retained, the table builds a
fresh current batch and reuses the immutable marks. This keeps metadata safe
across data-cache eviction without serving stale row data.

## Correctness And Invalidation

Marks are created only when the configured primary field is numeric, complete,
and ordered in physical row order. Composite marks additionally require every
configured field to be numeric and the tuple sequence to be complete and
lexicographically ordered. Nulls or out-of-order updates disable the primary
mark for that layout. Every table mutation clears both the full layout cache
and the sparse-mark cache before changing rows, so a mark cannot prune a new
snapshot with old bounds.

`BorrowSQLColumnarSourceSegments` returns the reused marks together with the
fresh batch. Existing SQL execution uses them only for direct numeric range
predicates; all other queries retain the ordinary scan path. If the mark is
missing, invalid, or too large for the configured limit, the normal resolver is
used.

## Cost And Limits

The default-disabled configuration has no mark-cache allocation or lookup.
With the mark cache enabled, its retained memory is bounded by
`SparsePrimaryMarkMaxBytes`; the benchmarked 65,536-row layout retained only
the primary bounds rather than the full data layout. Composite marks retain
more metadata than a single-field mark, but remain inside the same explicit
budget and add no per-query allocations. The selective-query benchmark reduced
per-query bytes from about 1.58 MiB to 1.576 MiB and reduced allocations by six
per operation while making the query about 3.41x faster. The composite
layout-eviction benchmark measured a smaller 1.04x CPU improvement with
unchanged query bytes and allocations; its benefit is preserving multi-column
pruning after the full layout is evicted, not reducing the retained mark
payload.

This is an in-memory read optimization. Marks are rebuilt after table restore
or recreation and are not a backup or replication format.
