# T223: Functional Indexes Over Derived Expressions

This round-2 idea was already present in the repository. The existing code
covers low-level typed functional indexes, conditional functional indexes, and
the materialized SQL `LOWER(...)` expression path. This record refreshes the
verification and makes the cost of maintaining the derived index explicit; it
does not add a duplicate implementation.

## Implementations

- `hatDataStructure.FunctionalIndex` maps an extracted comparable key to row
  IDs and supports lookup, update, delete, duplicate-key handling, and reusable
  result buffers.
- `hatDataStructure.ConditionalFunctionalIndex` admits rows through a
  validated predicate before indexing the derived key.
- `hatSchema.MaterializedSource.BuildFunctionalIndex` publishes a
  generation-checked materialized SQL index and maintains it for later inserts.
- SQL expression resolution recognizes supported registered expressions such
  as `LOWER(field)` and preserves the existing scan fallback for unsupported
  expressions.

The detailed API and lifecycle documentation remains in
[FUNCTIONAL_INDEX.md](FUNCTIONAL_INDEX.md),
[TR023_FUNCTIONAL_INDEX.md](TR023_FUNCTIONAL_INDEX.md), and
[TG11_CONDITIONAL_INDEX.md](TG11_CONDITIONAL_INDEX.md).

## Correctness Verification

The existing tests cover duplicate keys, update/delete maintenance, scratch
buffer reuse, nil and missing-extractor rejection, concurrent mixed operations,
SQL expression execution, and generated-SQL differential behavior:

```text
make test-t223
make race-t223
make vet-t223
```

The focused package tests, SQL expression verification, race checks, and vet
checks all passed before this documentation change.

## Benchmark

Fresh runs used `-benchmem -count=5` on Linux/amd64 with an AMD Ryzen 9 5950X.
The values below are medians from the raw runs recorded in `BENCHMARK.md`.

### Materialized functional SQL index

| Workload | ns/op | bytes/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Full SQL scan | 24,805,323 | 9,986,633 | 70,235 | 1.00x |
| Functional-index lookup | 140,285 | 113,420 | 734 | 176.8x faster; 88.0x fewer bytes; 95.7x fewer allocations |
| Equivalent scan control | 646,951 | 80,000 | 10,000 | Control for the derived-key workload |
| Functional-index build | 8,762,816 | 4,836,936 | 50,845 | 5.24x slower and 4.51x more bytes than equivalent scan-build control |

The query path is a large win for repeated predicates, but building and
maintaining the materialized index is materially more expensive than scanning
the derived values once. It should remain opt-in and be used when read reuse
amortizes that cost.

### SQL `LOWER(...)` expression resolution

| Workload | ns/op | bytes/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Equality scan | 15,363,862 | 7,971,049 | 135,249 | 1.00x |
| Equality via lower index | 134,798 | 113,960 | 750 | 114.0x faster; 70.0x fewer bytes; 180.3x fewer allocations |
| Literal-`IN` scan | 18,803,990 | 7,729,570 | 135,650 | 1.00x |
| Literal-`IN` index union | 301,683 | 325,768 | 2,038 | 62.3x faster; 23.7x fewer bytes; 66.6x fewer allocations |

Reproduce the measurements with:

```text
make benchmark-t223
```

## Limits and tradeoffs

Derived keys must be deterministic and registered through the supported API;
arbitrary planner inference is intentionally not enabled. Inserts and updates
must evaluate the expression and maintain postings, and snapshot/load paths
must rebuild or validate derived state. The benchmarked build penalty is the
reason the feature remains opt-in instead of replacing ordinary scans by
default.
