# M214 Arrangement Reuse Audit

## Decision

No additional runtime implementation is needed for M214. Arrangement reuse is
already present in two bounded, ownership-aware layers:

- `sqlPrepareArrangementWorkloads` stores immutable arrangement workload
  fingerprints on compiled SQL plans.
- `SQLCompiledQueryCache` reuses equivalent canonical token streams and exact
  source plans with bounded LRU limits.
- `TypedTableAggregateArrangements` shares exact aggregate arrangement state
  through explicit leases and reference counts.
- `TypedTableJoinArrangements` does the same for compatible join definitions.

Adding a process-wide arrangement registry would make invalidation, table
lifecycle, checkpoint restore, and memory ownership harder to reason about.
The existing table-local registries provide the reuse benefit without that
cross-lifecycle risk. Join definitions are intentionally not normalized here:
the constructor currently rejects whitespace-variant field names, so changing
the registry key alone would silently broaden API semantics rather than being a
pure optimization.

## Correctness

Command:

```text
make test-m214-arrangement-reuse-audit
```

Result: all `TestMZ045*` tests passed. The tests cover immutable arrangement
workload reuse, equivalent compiled-plan cache hits, literal preservation, and
canonical-entry eviction.

## Measurements

Five `-benchmem` samples ran on Linux amd64 on an AMD Ryzen 9 5950X.

| Workload | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Compiled arrangement workload reuse | 5,237 | 4,272 | 42 | baseline for this comparison |
| Recomputed arrangement workload | 7,560 | 4,920 | 62 | reuse is 1.44x faster, 13.2% lower bytes, 32.3% fewer allocations |
| Equivalent plan compile without canonical cache | 660,050 | 477,830 | 2,079 | baseline for this comparison |
| Equivalent plan compile with canonical cache | 211,567 | 176,476 | 597 | 3.12x faster, 2.71x lower bytes, 3.48x fewer allocations |
| Exact compiled-plan cache hit | 25.96 | 0 | 0 | allocation-free hit |

Raw samples can be reproduced with:

```text
make benchmark-m214-arrangement-reuse-audit
```

This audit closes M214 without changing live query behavior or adding a new
global retention policy.
