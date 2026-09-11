# SQL Partition Range Pruning

`PartitionPruningSourceResolver` is an optional SQL source contract for
partition-aware providers. The planner forwards a predicate only when it can
prove that the predicate is a literal, binary-collation condition on a direct
field of the source:

- `field = literal`
- `field IN (literal, ...)`
- `field < literal`, `field <= literal`, `field > literal`, or `field >= literal`
- the same comparison with the literal on the left; the operator is reversed
  before it reaches the provider

The resolver must return every partition that could contain a matching row.
The SQL executor still evaluates the complete original `WHERE`, so a provider
can conservatively return extra partitions. Returning `available=false` keeps
the existing partitioned-source or full-source path. `OR`, computed fields,
`NULL` literals, `TABLESAMPLE`, and non-binary collations do not produce a
pruning predicate.

```go
type PartitionPruningSourceResolver interface {
    ResolveSQLSourcePartitionsForPredicate(
        name string,
        key string,
        predicate SQLPartitionPredicate,
    ) ([]SQLSourcePartition, bool, error)
}
```

For example, a range-partitioned provider can map
`WHERE event_at >= '2026-02-01T00:00:00Z'` to February and later partitions.
The interface does not impose a physical layout, partition key type, or
automatic partition configuration; those remain provider-owned and opt-in.

## Measurement

Run:

```sh
make benchmark-sql-range-partition-pruning
```

The benchmark uses 64 deterministic partitions with 128 rows each. The range
predicate matches only the final partition. The values below are five samples
on an AMD Ryzen 9 5950X Linux host; `B/op` is Go allocation volume per query,
not retained RSS.

| Path | Without range pruning | With range pruning | Improvement |
| --- | ---: | ---: | ---: |
| `ns/op` | 711992; 709268; 710291; 726288; 726353 | 41542; 39678; 40075; 39976; 40134 | 17.77x lower median latency |
| `B/op` | 973940; 973936; 973940; 973936; 973936 | 70000; 70000; 70000; 70000; 70000 | 13.91x lower median allocation volume |
| `allocs/op` | 282; 282; 282; 282; 282 | 281; 281; 281; 281; 281 | 1 allocation/query lower |

Median latency is `711992 ns/op` without pruning versus `40075 ns/op` with
pruning. The benchmark isolates provider-side partition selection; physical
part/mark metadata and automatic partition maintenance are deliberately not
claimed by this contract.
