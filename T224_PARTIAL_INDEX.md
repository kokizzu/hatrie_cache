# T224: Partial Indexes With Validated Predicates

The round-2 partial-index idea was already implemented as an opt-in SQL JSON
partial index and as a conditional functional/index catalog. This record
refreshes correctness and performance evidence; it does not add a duplicate
runtime implementation.

## Implementations

- `hatDataStructure.ConditionalFunctionalIndex` admits only rows accepted by a
  predicate, then indexes the derived key. Admission and replacement are
  bounded and atomic.
- `hatDataStructure.ConditionalIndexCatalog` stores validated definitions and
  planner metadata, fences writes during rebuilds, invalidates on generation
  changes, and atomically publishes a replacement.
- `CreateSQLJSONPartialIndex` maintains the supported SQL predicate/index path
  and leaves unsupported predicates on the existing scan fallback.

Detailed API and lifecycle documentation remains in
[TG11_CONDITIONAL_INDEX.md](TG11_CONDITIONAL_INDEX.md),
[TU24_CONDITIONAL_INDEX_CATALOG.md](TU24_CONDITIONAL_INDEX_CATALOG.md), and
[SQL_PARTIAL_INDEX.md](SQL_PARTIAL_INDEX.md).

## Correctness Verification

The focused checks cover predicate admission, replacement, lookup/clear,
concurrent use, catalog metadata validation, atomic rebuilds, fenced writes,
generation invalidation, SQL partial-index refresh, and scan fallback:

```text
make test-t224
make race-t224
make vet-t224
```

All focused tests, SQL verification, race checks, and vet checks passed before
this documentation change.

## Benchmark

Fresh runs used `-benchmem -count=5` on Linux/amd64 with an AMD Ryzen 9 5950X.
The values below are medians from the raw runs recorded in `BENCHMARK.md`.

### SQL partial-index refresh

| Workload | ns/op | bytes/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Refresh rows accepted by partial predicate | 276,529 | 25,929 | 1,013 | 6.37x faster; 22.0x fewer bytes; 19.8x fewer allocations |
| Rebuild composite index for all rows | 1,760,607 | 568,330 | 20,031 | Baseline |

The selective path avoids indexing rejected rows, so the gain depends on
predicate selectivity. A predicate that accepts nearly every row will reduce the
refresh advantage while still carrying predicate evaluation and maintenance
cost.

### Direct conditional index

The direct conditional-index benchmarks compare predicate admission and lookup
with the corresponding pre-catalog controls. They are retained separately from
the SQL refresh measurement so catalog and planner overhead is visible.

| Workload | ns/op | bytes/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Pre-catalog upsert | 31.19 | 0 | 0 | Baseline |
| Catalog upsert | 43.10 | 0 | 0 | 1.38x slower |
| Pre-catalog lookup | 30.35 | 8 | 1 | Baseline |
| Catalog lookup | 44.12 | 8 | 1 | 1.45x slower |
| Conditional index build | 456,452 | 238,539 | 26 | Build cost reference |

## Limits and tradeoffs

Predicates must use the supported validated form; arbitrary expressions are not
silently inferred or indexed. Predicate changes require a rebuild, and a
generation change invalidates stale catalog state. Rebuilds publish atomically,
so a canceled or failed build cannot expose a partially populated index, at the
cost of temporary rebuild work and memory.
