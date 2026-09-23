# CH-G01 Ordered Partial-Merge Join

This is the first measured part of the ClickHouse-inspired CH-G01 join work.
It adds an opt-in ordered partial-merge implementation for inner equality joins.
It does not yet enable automatic algorithm selection or grace-hash spill.

## Configuration

Set the option on one query:

```go
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	MaxRows:       100000,
	JoinAlgorithm: hatSql.SQLJoinAlgorithmPartialMerge,
})
```

The default empty value and `SQLJoinAlgorithmHash` retain the existing planner.
`partial_merge` is used only when the resolver implements
`SQLOrderedSourceResolver` and reports the right source as available. Otherwise
the executor falls back to the established index, hash, or nested-loop path.

The current implementation supports inner joins whose `ON` clause is one
binary equality between fields. It skips NULL keys, preserves duplicate-key
Cartesian matches, observes `MaxRows` and join work cancellation, and restores
the existing source-order result convention. Right-side pushed predicates and
outer joins continue through the existing paths.

## Algorithm

The right source is requested in ascending key order. The executor checks
whether the current left rows are already ordered; if not, it makes a slice
copy and sorts that slice by the join key. A two-pointer merge then retains no
hash table. Equal-key runs are expanded to preserve duplicate matches. This
reduces retained hash-index metadata and allocations, but the source rows and
result rows are still materialized. A future streaming ordered resolver can
remove more input retention.

## Measurement

Workload: two 4,096-row `CACHE` sources, one unique `int64` equality key per
row, five samples with `-benchtime=2s`, `-benchmem`, on the repository's AMD
Ryzen 9 5950X host. Both paths use the same resolver implementation; the
baseline leaves `JoinAlgorithm` at its default.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU | Relative memory | Relative allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Existing hash join | 6,184,046 | 9,817,381 | 49,230 | 1.00x | 1.00x | 1.00x |
| Opt-in partial merge | 5,781,669 | 6,659,298 | 36,908 | 1.07x faster | 1.47x lower | 1.33x lower |

The measured path is about 6.5% faster, uses about 32.2% fewer allocated
bytes, and performs about 25.0% fewer allocations for this workload. The
result is workload-dependent: sorting an unsorted left side and relying on an
ordered source resolver can cost more than a hash join for small or skewed
inputs. The feature therefore remains opt-in until a broader adaptive policy
has representative measurements.

Raw samples:

```text
BenchmarkCH001HashJoinBaseline
5955048 9817393 49230
6234442 9817381 49230
5990453 9817380 49230
6184046 9817377 49230
6794796 9817383 49230

BenchmarkCH001PartialMergeJoin
5908251 6659298 36908
5555882 6659297 36908
5769898 6659298 36908
5781669 6659296 36908
5843252 6659299 36908
```

Run the repeatable measurements with:

```text
make m240-ch-g01-test
make m240-ch-g01-benchmark
```

The focused tests cover duplicate keys, NULL non-matches, unsorted left rows,
default-off behavior, and fallback when ordered reads are unavailable.
