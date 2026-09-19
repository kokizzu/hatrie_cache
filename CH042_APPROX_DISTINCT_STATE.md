# Mergeable Approximate Distinct SQL States

Status: implemented.

This feature adds a mergeable SQL state path for approximate distinct counts:

```sql
APPROX_COUNT_DISTINCT_STATE(value[, precision])
APPROX_COUNT_DISTINCT_MERGE(state)
```

`APPROX_COUNT_DISTINCT_STATE` ignores `NULL` values and returns the versioned,
checksummed `HAG1` HyperLogLog state used by `hatDataStructure.HyperLogLog`.
`APPROX_COUNT_DISTINCT_MERGE` accepts those `[]byte` states and combines them
without replaying the original values. States with different precision values,
invalid encodings, or the wrong SQL value type are rejected. An empty merge
returns `0`, matching the materialized approximate-count result.

The direct no-`WHERE` scan uses the existing streaming aggregate executor. The
state is encoded once when the aggregate result is materialized, so the SQL
state form does not retain one serialized copy per input row.

The design is similar in purpose to ClickHouse's mergeable approximate
`uniq` aggregate, which uses bounded approximate state for distinct counting:
[ClickHouse `uniq` documentation](https://clickhouse.com/docs/reference/functions/aggregate-functions/uniq).
The SQL names here make the partial-state boundary explicit for storage,
transport, and later merge operations.

## Semantics

- Duplicate non-`NULL` values are counted according to HyperLogLog semantics.
- `NULL` values do not enter the sketch.
- The optional precision must be the same for all states being merged.
- A state is self-describing and checksummed; callers should treat `HAG1` as a
  versioned application state format and preserve it unchanged between state
  production and merge.
- Empty input produces an empty state; merging only empty states produces `0`.

## Measurements

The benchmark uses the same 20,000-row input for the existing approximate
aggregate and the new state path. Values are medians from five benchmark
samples; lower is better.

| Path | ns/op | B/op | allocs/op | wire bytes/op | CPU vs baseline | Memory vs baseline |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Existing `APPROX_COUNT_DISTINCT` | 1,417,441 | 166,592 | 19,998 | - | 1.00x | 1.00x |
| `APPROX_COUNT_DISTINCT_STATE`, streaming | 1,409,937 | 167,857 | 20,001 | 1,051 | 0.99x | 1.01x |
| `APPROX_COUNT_DISTINCT_MERGE` | 44,498 | 23,120 | 57 | - | separate merge workload | separate merge workload |

An initial materialized implementation was rejected after measurement: it was
4,312,596 ns/op, 4,734,137 B/op, and 40,004 allocs/op. The accepted streaming
implementation is about 2.75x faster, uses about 28.2x less benchmark memory,
and uses about half as many allocations as that first attempt, while keeping
the existing approximate-count cost effectively unchanged.

Run the reproducible checks with:

```text
make test-chu42
make test-chu42-package
make race-chu42
make vet-chu42
make benchmark-chu42
```
