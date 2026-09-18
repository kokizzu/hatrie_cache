# TR-030 Materialized Index Statistics

TR-030 exposes current distribution metadata for equality indexes maintained by
`hatSchema.MaterializedSource`. The existing SQL planner uses it only when an
`AND` predicate has competing indexed equality terms, so it can probe the
smallest posting list before evaluating the complete predicate.

## Public API

`hatSchema.SQLResolverAdapter` implements:

- `SQLJSONIndexStats(key, fields...)`, which returns `hatSql.JSONIndexStats`.
- `SQLJSONIndexValueEstimate(key, field, value)`, which returns the exact
  posting-list length without cloning matching rows.

The corresponding source methods are `MaterializedSource.IndexStats` and
`MaterializedSource.IndexValueEstimate`. The adapter delegates non-materialized
sources to an optional base resolver, so existing resolver composition remains
possible.

`JSONIndexStats` contains total rows, null rows, distinct non-null keys,
minimum/maximum/average posting frequency, and a compact frequency histogram.
Regular and covering indexes count null source fields separately and exclude
their null posting from the non-null distribution. Functional indexes report
the evaluator's maintained posting distribution and do not infer SQL nulls from
the source row.

Statistics are cached per field and invalidated on inserts and on publication
of a secondary, covering, or functional index. A generation check prevents a
concurrent index build from publishing statistics for an obsolete row snapshot.
Missing or unsupported indexes return `available=false`; no values are exposed
through the statistics API.

## Planner behavior

For multiple equality conjuncts, the planner estimates each available index,
sorts by estimated posting size, and probes the smallest candidate. The full
predicate is still evaluated after the probe, so statistics can change work but
cannot change result correctness. A single direct equality keeps the existing
direct probe path: no distribution-statistics or value-estimate lookup is
performed because there is no competing equality index to choose.

Index statistics are observational and do not change storage or wire formats.
The feature is available automatically when a caller uses the existing
`SQLResolverAdapter` with a maintained materialized index; sources without an
index or without the adapter retain their prior fallback behavior.

## Measurement

Command:

```text
make benchmark-tr030-index-stats
```

Five `-benchmem` samples were run on Linux/amd64 with an AMD Ryzen 9 5950X.
The fixture contains 10,000 rows and indexed `id` and `kind` fields. The
multi-index query asks for the rare `id` while also filtering the common
`kind` value.

| Workload | Before median | After median | Result |
| --- | ---: | ---: | --- |
| Two competing equality indexes | 6.064 ms/op, 5,802,820 B/op, 20,032 allocs/op | 15.317 us/op, 11,202 B/op, 69 allocs/op | **395.6x faster**, 518.0x fewer bytes, 290.3x fewer allocations |
| One equality index control | 8.166 us/op, 6,029 B/op, 33 allocs/op | 8.230 us/op, 6,085 B/op, 35 allocs/op | 0.8% slower, 0.9% more bytes, 6.1% more allocations |

The control compares the old direct resolver with the adapter-backed resolver,
so its small residual difference includes adapter dispatch. The direct planner
path performs no statistics or value-estimate lookup. The meaningful cost is
the first distribution-statistics request after a mutation; later requests hit
the bounded cache.

Raw samples:

```text
BenchmarkTR030BeforeMaterializedIndexStats:
6063578 5803014 20032
5642041 5802820 20032
5849286 5802759 20032
6599737 5802808 20032
6881158 5802827 20032

BenchmarkTR030AfterMaterializedIndexStats:
15317 11202 69
15726 11202 69
14912 11202 69
15177 11202 69
15679 11202 69

BenchmarkTR030BeforeMaterializedIndexStatsSinglePredicate:
8321 6029 33
8061 6029 33
8701 6029 33
8099 6029 33
8166 6029 33

BenchmarkTR030AfterMaterializedIndexStatsSinglePredicate:
8388 6085 35
8068 6085 35
8227 6085 35
8230 6085 35
8518 6085 35
```

Focused correctness, race, vet, and package checks:

```text
make test-tr030-index-stats
make race-tr030-index-stats
make vet-tr030-index-stats
make package-tr030-index-stats
```
