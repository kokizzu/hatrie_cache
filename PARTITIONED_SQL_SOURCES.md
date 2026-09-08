# Partitioned SQL Sources

`hatSql` now accepts an optional `PartitionedSourceResolver` alongside the
existing `SourceResolver`. It lets an application expose several physical
partitions as one logical `CACHE(...)` or `KEYS(...)` source.

```go
type resolver struct {
	regions map[string][]hatSql.Row
}

func (r resolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	return nil, nil // legacy fallback for sources without partitions
}

func (r resolver) ResolveSQLSourcePartitions(name, key string) ([]hatSql.SQLSourcePartition, bool, error) {
	return []hatSql.SQLSourcePartition{
		{Name: "apac", Rows: r.regions["apac"]},
		{Name: "eu", Rows: r.regions["eu"]},
	}, true, nil
}
```

## Semantics

- The executor calls the partition method first for `CACHE` and `KEYS`.
- `available=false` falls back to the existing borrowed or ordinary resolver
  path, preserving old implementations and mixed source registries.
- `available=true` makes the returned partitions the complete logical source;
  an empty partition list is therefore an empty source, not a fallback request.
- Partitions are consumed in the order returned. Applications should return a
  deterministic order when query results need deterministic scan order.
- Rows are read-only for the lifetime of the query. The executor may retain the
  row slice during that query and avoids a defensive source clone, matching the
  existing borrowed-source contract.
- Rows are appended, not deduplicated. Duplicate primary keys or duplicate
  values across partitions remain visible to SQL operators.
- A partition-resolution error aborts the query and never falls back to a
  partial or ordinary source.

This is an opt-in API. Existing resolvers do not need a new method, and the
existing index, columnar, stream, and historical resolver extensions remain
available when implemented by the resolver passed to the query. The partition
adapter is a row-source boundary. Automatic literal predicate pruning is
documented in [PARTITION_PRUNING.md](PARTITION_PRUNING.md); per-partition
index/columnar fan-out, ownership consensus, and cross-partition writes remain
separate follow-up features.

## Benchmark

Command:

```text
make benchmark-partitioned-source-local-clean
```

Machine: AMD Ryzen 9 5950X, Linux amd64. Five benchmark samples were run for
the same `COUNT(*)` query over 2,048 rows. The partitioned case used eight
physical partitions and the immutable row contract; the regular case used the
legacy resolver path, including its normal defensive query-cache clone.

| Path | Median ns/op | B/op | allocs/op | Relative CPU | Relative bytes | Relative allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Legacy single-source resolver | 464,314 | 923,006 | 4,120 | 1.00x | 1.00x | 1.00x |
| Partitioned immutable source | 68,282 | 234,873 | 24 | 6.80x faster | 3.93x lower | 171.67x fewer |

The comparison measures the complete SQL execution path, so the improvement
comes largely from the explicit immutable-source guarantee that avoids copying
source rows into the per-query cache. The remaining cost is one combined row
slice when more than one partition is returned. The feature is opt-in and
does not change the cost or behavior of legacy resolvers.
