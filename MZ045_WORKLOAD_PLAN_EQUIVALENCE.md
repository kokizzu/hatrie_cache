# MZ045 Workload Plan Equivalence

## Status

Implemented as a bounded, immutable compiled-query optimization.

## Behavior

`CompileSQLQuery` now computes the normalized `SQLArrangementWorkload` once
for the immutable query tree. `EXPLAIN`, `EXPLAIN PIPELINE`, and arrangement
recommendation reads reuse that fingerprint for the scan and join steps. CTEs,
subqueries, joins, and unions in the compiled tree are prepared before the
compiled handle is published.

The existing `SQLCompiledQueryCache` continues to keep equivalent token
streams in the same bounded compiled-plan entry. Literal values and schema
versions remain separate namespaces, so this optimization does not share
parameter-dependent execution state.

Parameterized executions clone the compiled template and clear the cached
workload before binding and rewrite. This is required because a parameter-
dependent rewrite may change the query shape.

## Measurement

The paired benchmark uses the same multi-join query and repeats explain
planning on an AMD Ryzen 9 5950X:

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Parsed workload recomputed per explain | 6,459 | 4,920 | 62 |
| Compiled workload reused | 4,812 | 4,272 | 42 |

That is about `1.34x` lower explain latency, `13%` less heap, and `32%`
fewer allocations for repeated compiled explains. Compilation pays the
one-time workload preparation cost; the optimization is intended for handles
that are executed or explained more than once.

## Verification

The focused targets are:

```text
make test-mz045-arrangement-workload
make benchmark-mz045-arrangement-workload
make race-mz045-arrangement-workload
make vet-mz045-arrangement-workload
```

The regression test verifies precomputation, repeated reuse, and invalidation
on parameter-bound clones.
