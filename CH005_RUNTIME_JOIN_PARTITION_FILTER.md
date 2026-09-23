# CH-G05 Runtime Join Partition Bounds

Hatrie SQL can optionally derive an inclusive min/max range from the already
materialized left side of an eligible inner equality hash join and pass that
range to a partition-aware right-side resolver. This is useful when the right
source is remote or split into many physical partitions and the current left
keys occupy only a small range.

## Configuration

The feature is disabled by default:

```go
options := hatSql.SQLQueryOptions{
	RuntimeJoinPartitionFilter: true,
}
```

The option is used only when the query has an inner equality hash join, the
right source has not already been pushed into the query, and the left rows
produce homogeneous non-NULL numeric, string, boolean, or `time.Time` bounds.
Unsupported values, mixed types, an empty usable key set, or `NaN` disable the
optimization for that join.

## Resolver contract

Partitioned resolvers can implement the optional interface:

```go
type RuntimeJoinPartitionPruningSourceResolver interface {
	ResolveSQLSourcePartitionsForJoinBounds(
		name string,
		key string,
		bounds SQLRuntimeJoinBounds,
	) ([]SQLSourcePartition, bool, error)
}
```

`SQLRuntimeJoinBounds.Field` is the right-side join field. `Min` and `Max` are
inclusive values observed on the left side. Returning `available=false` keeps
the ordinary source resolver path. A resolver returning `available=true` must
return every partition whose recorded range could overlap the bounds; it may
return extra partitions. The executor still evaluates the exact equality join,
so this interface is an I/O optimization rather than a second SQL semantics
implementation.

The normal source resolver is still called for the left input. When the
runtime filter is enabled and available, only the selected right partitions
are flattened. The option does not change outer joins, right/full joins,
non-equality joins, pushed sources, row limits, or output ordering.

## Measurement

Command:

```text
make m258-ch-g05-benchmark
```

The benchmark uses 128 left rows and 128 right partitions of 256 rows each;
all left keys fall into one right partition. It runs five samples with
`-benchtime=2s -benchmem -count=5` on an AMD Ryzen 9 5950X.

| Path | Median ns/op | Median B/op | Median allocs/op | Improvement |
| --- | ---: | ---: | ---: | --- |
| Existing full right-source materialization | 15,864,057 | 18,345,703 | 99,641 | Reference |
| Runtime min/max partition pruning | 174,885 | 281,329 | 1,463 | 90.7x faster; 65.2x lower cumulative bytes; 68.1x fewer allocations |

This is a selective-partition workload. If most partitions overlap the left
range, the resolver may return nearly the full source and the option adds a
small bounds scan, so it remains opt-in.

Raw samples:

```text
BenchmarkCH005RuntimeJoinPartitionFilterBaseline-32
14116109 ns/op 18345725 B/op 99640 allocs/op
14500407 ns/op 18345699 B/op 99641 allocs/op
15864057 ns/op 18345703 B/op 99641 allocs/op
17341750 ns/op 18345711 B/op 99641 allocs/op
16039629 ns/op 18345712 B/op 99641 allocs/op

BenchmarkCH005RuntimeJoinPartitionFilter-32
202824 ns/op 281329 B/op 1463 allocs/op
178847 ns/op 281329 B/op 1463 allocs/op
170360 ns/op 281329 B/op 1463 allocs/op
174885 ns/op 281329 B/op 1463 allocs/op
174591 ns/op 281329 B/op 1463 allocs/op
```

Correctness coverage includes duplicate-preserving result comparison,
partition-bound selection, default-off behavior, mixed-key fallback, and the
unchanged outer-join path:

```text
make m256-ch-g05-test
```
