# Cross-Partition Ordered Pagination

`hatSql` can merge ordered physical partitions into one keyset-paginated SQL
result. This is an opt-in extension for explicit partitioning: it does not
discover partitions, move rows, elect owners, or change the default routing
path.

## Resolver Contract

Implement `PartitionedOrderedSourceResolver` alongside `SourceResolver`:

```go
type resolver struct {
	partitions []hatSql.SQLSourcePartition
}

func (r resolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	return nil, nil // legacy fallback for non-paginated queries
}

func (r resolver) ResolveSQLOrderedSourcePartitions(
	name, key, field string,
	desc, nullsFirst, nullsLast bool,
) ([]hatSql.SQLSourcePartition, bool, error) {
	return r.partitions, true, nil
}
```

Each partition must have a stable, non-empty, unique `Name`. Its `Rows` must be
in the requested `ORDER BY` order, including the requested direction and NULL
placement. The executor validates that order before merging and evaluates the
complete `WHERE` predicate again, so partitions may safely contain rows that
the predicate excludes.

## Query Behavior

The existing `ExecuteSQLQueryKeysetPage` API automatically selects the merge
when the resolver implements this contract and returns `available=true`.
Supported queries retain the existing keyset restrictions: one direct
`CACHE` source, one field `ORDER BY`, and no joins, grouping, distinct,
windows, CTEs, or unions. The merge uses a bounded heap with one current row
per partition rather than flattening and globally sorting every row.

The opaque cursor records the query fingerprint, returned-row count, partition
names, and the next row offset in every partition. A reordered, renamed, or
shortened partition list is rejected rather than returning duplicate or
missing rows. A resolver returning `available=false` falls back to the
existing direct keyset stream when one is available; old resolvers do not need
to implement the new method. `CatalogResolver` and `SQLSession` forward the
capability while preserving their existing virtual-source and temporary-table
precedence.

## Benchmark

Command:

```text
make benchmark-t084-local-clean
```

Machine: AMD Ryzen 9 5950X, Linux amd64. Five samples compared the first page
of 100 rows from 16 ordered partitions containing 2,048 rows each (32,768
rows total). Fixture construction was outside the timed region.

Raw samples (`ns/op`, `B/op`, `allocs/op`):

| Path | Sample 1 | Sample 2 | Sample 3 | Sample 4 | Sample 5 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing flattened offset page | 41,887,661 / 19,149,951 / 98,350 | 46,525,510 / 19,148,379 / 98,348 | 43,503,654 / 19,148,813 / 98,349 | 49,030,649 / 19,149,169 / 98,350 | 47,686,189 / 19,148,090 / 98,348 |
| Partitioned keyset merge | 834,301 / 81,470 / 753 | 858,794 / 81,470 / 753 | 973,847 / 81,475 / 753 | 837,272 / 81,470 / 753 | 789,076 / 81,471 / 753 |

| Median | ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: |
| Existing flattened offset page | 46,525,510 | 19,148,813 | 98,349 | 1.00x |
| Partitioned keyset merge | 837,272 | 81,470 | 753 | 55.6x faster, 235.0x fewer bytes, 130.6x fewer allocations |

The benchmark includes validation of the already ordered partition rows and
the normal SQL predicate/projection work. It does not claim that the resolver's
partition snapshots consume no memory; those snapshots are supplied outside
the timed region. The gain is avoiding a combined row slice, global sort, and
their per-page allocations.
