# Materialize M-U39: Ordered Partition SQL Streaming

Status: adopted as an opt-in execution fast path.

## What It Does

`ExecuteSQLQueryRows` can merge independently ordered physical partitions with a
small k-way heap instead of flattening the source and running a global sort.
The merge stops after `LIMIT` rows, while still applying `PREWHERE`, `WHERE`,
`OFFSET`, and the select-list projection in SQL order.

The existing `PartitionedOrderedSourceResolver` contract is used:

```go
type OrderedResolver struct { /* owns immutable partitions */ }

func (r *OrderedResolver) ResolveSQLOrderedSourcePartitions(
	name, key, field string,
	desc, nullsFirst, nullsLast bool,
) ([]hatSql.SQLSourcePartition, bool, error) {
	// Return each partition sorted by the requested field and direction.
}

err := hatSql.ExecuteSQLQueryRows(
	ctx,
	`FROM CACHE('events') SELECT id, score ORDER BY score LIMIT 100`,
	r, nil, hatSql.SQLQueryOptions{}, visit,
)
```

The resolver owns the partition rows and must keep them immutable during the
query. The executor validates partition names and per-partition ordering before
merging. Invalid ordered partitions return an error rather than silently
returning incorrect SQL results.

## Fast-Path Shape

The optimization is used only for a direct, single-source query with one field
`ORDER BY` expression. It supports binary/default collation, projections,
`PREWHERE`, `WHERE`, `OFFSET`, `LIMIT`, ascending order, and descending order.

The normal path remains unchanged for joins, aggregates, windows, `DISTINCT`,
CTEs, unions, `LIMIT BY`, `WITH TIES`, multiple sort keys, computed sort
expressions, and non-binary collations. A resolver that does not implement the
ordered-partition method, or returns `available=false`, also uses the existing
source, top-N, external-sort, or materialized fallback.

## Measurement

Run:

```text
make bench-mz039-partitioned-order
```

The benchmark uses 16 partitions and 65,536 total rows with `LIMIT 64` on
Linux/amd64 and an AMD Ryzen 9 5950X. Both paths use `ExecuteSQLQueryRows`; the
baseline exposes one flat source and the optimized path exposes ordered
partitions. The partition fixture itself is created outside the timed region.

| Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing streamed top-N fallback | 18,946,103; 18,861,964; 18,841,807; 18,436,368; 18,329,873 | 18,841,807 | 29,390,871 | 197,089 |
| Ordered partition merge | 1,589,182; 1,796,690; 1,830,891; 1,695,793; 1,517,262 | 1,695,793 | 62,751 | 872 |

The ordered merge is 11.11x faster, uses 468.37x fewer transient allocation
bytes, and makes 226.02x fewer allocations in this workload. `B/op` is Go
allocation traffic, not process RSS; partition storage remains owned by the
resolver and is outside the benchmarked operation.

Focused correctness coverage includes ascending and descending merges, filters,
offsets, projections, default binary collation, fallback behavior, and
non-binary-collation rejection.
