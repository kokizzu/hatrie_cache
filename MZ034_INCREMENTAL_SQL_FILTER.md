# MZ-034 Incremental SQL Filter

Materialize-style differential filter lowering is now available through
`CompiledSQLQuery.CompileIncrementalFilter`.

## Scope

The adapter accepts the exact, bounded shape:

- one `CACHE(...)` or `VALUES` source;
- `SELECT *` projection;
- an optional scalar `WHERE` expression;
- no joins, grouping, ordering, limits, windows, CTEs, unions, custom
  functions, or unbound parameters.

`SQLIncrementalFilter.Apply` accepts signed `DifferentialRow` updates and
returns only rows whose predicate is true. It preserves key, timestamp, and
signed multiplicity, uses the normal SQL three-valued truth rules, clones row
payloads, and returns no partial output when a batch item is invalid.

Unsupported query shapes are rejected by the incremental compiler and remain
on the normal SQL executor. The operator is stateless and single-writer, so it
does not retain the source table or require a second copy of the input data.

## Benchmark

Command:

```text
make benchmark-mz034-incremental-sql-filter
```

Workload: rebuild a compiled `SELECT * FROM CACHE('events') WHERE active =
TRUE` over 10,000 rows, versus applying one signed update to the incremental
adapter. Each result was run five times with Go benchmark memory reporting;
the table uses the median sample from the final paired run.

| Path | ns/op | B/op | allocs/op | Relative latency | Relative transient bytes | Relative allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rebuild full SQL result | 4,882,078 | 5,244,322 | 45,028 | 1x | 1x | 1x |
| Incremental signed filter | 491.9 | 392 | 5 | **9,914x faster** | **13,378x lower** | **9,006x fewer** |

Raw final samples:

```text
BenchmarkMZ034RebuildSQLFilter-32       241 4882078 ns/op 5244322 B/op 45028 allocs/op
BenchmarkMZ034RebuildSQLFilter-32       250 4687581 ns/op 5244234 B/op 45028 allocs/op
BenchmarkMZ034RebuildSQLFilter-32       249 5036689 ns/op 5244339 B/op 45028 allocs/op
BenchmarkMZ034RebuildSQLFilter-32       248 5270908 ns/op 5244337 B/op 45028 allocs/op
BenchmarkMZ034RebuildSQLFilter-32       254 4852417 ns/op 5244290 B/op 45028 allocs/op
BenchmarkMZ034IncrementalSQLFilter-32 2257150 506.1 ns/op    392 B/op     5 allocs/op
BenchmarkMZ034IncrementalSQLFilter-32 2535722 486.7 ns/op    392 B/op     5 allocs/op
BenchmarkMZ034IncrementalSQLFilter-32 2377146 491.9 ns/op    392 B/op     5 allocs/op
BenchmarkMZ034IncrementalSQLFilter-32 2481942 503.7 ns/op    392 B/op     5 allocs/op
BenchmarkMZ034IncrementalSQLFilter-32 2427186 488.7 ns/op    392 B/op     5 allocs/op
```

## Tradeoffs

The comparison is intentionally a steady-state single-row update against a
10,000-row rebuild, so it measures the workload differential maintenance is
meant to avoid. It is not a claim that a full query should always be replaced:
the adapter rejects joins, aggregates, ordering, and other shapes whose
semantics need retained state. Each update still evaluates the SQL predicate
and allocates the returned row clone; callers that already own immutable rows
can account for that copy cost separately.

## Verification

```text
make test-mz034-incremental-sql-filter
make test-mz034-incremental-sql-filter-package
make race-mz034-incremental-sql-filter
make vet-mz034-incremental-sql-filter
```

The focused tests cover signed acceptance and retraction, SQL unknown
filtering, row ownership, unsupported-shape errors, missing-row errors, and
batch atomicity.
