# MZ-009 Validity Partition Pruning

`VALID_AT(at, valid_from, valid_to)` can now be forwarded to an existing
`hatSql.PartitionPruningSourceResolver` when all three arguments are safe
planner literals or direct fields. The resolver receives:

```go
SQLPartitionPredicate{
	Operator:       "VALID_AT",
	Values:         []interface{}{at},
	ValidFromField: "valid_from",
	ValidToField:   "valid_to",
}
```

The provider may return only partitions that can contain rows valid at `at`.
It must not omit a possibly matching partition. Returning `available=false`
keeps the existing partitioned-source or full-source path.

The original SQL predicate is always evaluated after partition resolution.
Providers may therefore return a conservative superset, and invalid rows in a
selected partition are still filtered normally. This feature does not manage
frontiers, clocks, partition metadata, or cross-node coordination; those remain
the source provider's responsibility.

## Example

```go
type eventsResolver struct{}

func (eventsResolver) ResolveSQLSourcePartitionsForPredicate(
	name, key string,
	predicate hatSql.SQLPartitionPredicate,
) ([]hatSql.SQLSourcePartition, bool, error) {
	if name != "CACHE" || key != "events" || predicate.Operator != "VALID_AT" {
		return nil, false, nil
	}
	return lookupValidityPartitions(
		predicate.Values[0],
		predicate.ValidFromField,
		predicate.ValidToField,
	), true, nil
}
```

The hook is intentionally opt-in and backward-compatible. Existing providers
that only implement `ResolveSQLSource` or
`ResolveSQLSourcePartitions` are unchanged.

## Measurement

Command: `make benchmark-mz009-validity-partition-pruning`.

The workload has 64 physical partitions. The first partition contains four
matching rows and one out-of-range row; the other 63 partitions contain 256
expired rows each. Median of five samples on Linux/amd64, AMD Ryzen 9 5950X:

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Post-change full-source fallback | 4,672,182 | 7,658,563 | 32,635 | 1.00x |
| `VALID_AT` partition pruning | 8,016 | 8,345 | 42 | **583.1x faster; 917.7x fewer bytes; 777.0x fewer allocations** |

The fallback remains available when the provider cannot prove partition
eligibility. The fast path's additional predicate metadata is negligible
compared with avoiding materialization of the 63 irrelevant partitions.

Raw output:

```text
BenchmarkMZ009ValidityPartitionPruningBaseline-32  240  4801276 ns/op  7659606 B/op  32647 allocs/op
BenchmarkMZ009ValidityPartitionPruningBaseline-32  249  4659854 ns/op  7658563 B/op  32635 allocs/op
BenchmarkMZ009ValidityPartitionPruningBaseline-32  242  4546846 ns/op  7659327 B/op  32644 allocs/op
BenchmarkMZ009ValidityPartitionPruningBaseline-32  255  4672182 ns/op  7657937 B/op  32627 allocs/op
BenchmarkMZ009ValidityPartitionPruningBaseline-32  249  4709527 ns/op  7658559 B/op  32635 allocs/op
BenchmarkMZ009ValidityPartitionPruningFastPath-32  130911  8016 ns/op  8346 B/op  42 allocs/op
BenchmarkMZ009ValidityPartitionPruningFastPath-32  137665  7964 ns/op  8344 B/op  42 allocs/op
BenchmarkMZ009ValidityPartitionPruningFastPath-32  133785  8037 ns/op  8345 B/op  42 allocs/op
BenchmarkMZ009ValidityPartitionPruningFastPath-32  134032  8103 ns/op  8345 B/op  42 allocs/op
BenchmarkMZ009ValidityPartitionPruningFastPath-32  137786  7983 ns/op  8344 B/op  42 allocs/op
```
