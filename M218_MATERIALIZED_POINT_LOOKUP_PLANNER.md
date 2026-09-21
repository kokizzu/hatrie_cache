# M218 Materialized Point Lookup Planner

## Decision

M218 adds `MaterializedViewPointLookupResolver`, an opt-in adapter that exposes
one M217 maintained point lookup to the existing SQL planner. It implements
the `EXTERNAL` source contracts used by the SQL executor:

- An equality predicate on the configured field is converted with the caller's
  `ValueKey` function and served from the maintained point arrangement.
- An unsupported field, operator, or value returns `available=false`, so the
  existing executor performs a complete arrangement/source scan.
- Point lookup candidates are still evaluated by the original SQL predicate
  before rows are published.
- A point miss returns `available=true` with no candidates, avoiding a full
  scan because the maintained index is complete.
- The adapter reads refreshed materialized snapshots atomically and clones
  rows through the existing view APIs.

`SourceName` is restricted to `EXTERNAL` because that is the SQL planner hook
that supports `LookupSourceResolver`. `SourceKey` defaults to the indexed view
name. The caller owns the lossless correspondence between `Key` and `ValueKey`;
conversion errors fall back to a scan rather than changing query results.
Optional counters are disabled by default. When enabled, they make planner
selection observable without adding atomic-counter overhead to the normal
adapter path.

## Correctness

Commands:

```text
make test-m218-point-lookup-planner
make race-m218-point-lookup-planner
```

The focused suite covers point hits, point misses without scans, non-equality
scan fallback, value-conversion fallback, refresh visibility, and planner
counters. The race suite passes.

## Measurements

Command:

```text
make benchmark-m218-point-lookup-planner
```

Five `-benchmem` samples ran on Linux amd64 on an AMD Ryzen 9 5950X against a
10,000-row materialized view and one equality predicate near the end of the
input.

| Planner path | Raw ns/op samples | Median ns/op | B/op | Allocs/op | Relative result |
| --- | --- | ---: | ---: | ---: | --- |
| Maintained point lookup | 8,931; 8,362; 9,172; 9,130; 8,744 | 8,931 | 6,580 | 30 | baseline |
| Full arrangement scan | 6,072,627; 6,941,275; 6,928,011; 7,586,531; 7,231,491 | 6,941,275 | 8,012,628 | 40,030 | point plan is 777.2x faster; 1,217.7x lower bytes; 1,334.3x fewer allocations |

The benchmark includes SQL parsing, source adaptation, predicate evaluation,
projection, and result cloning in both paths. The point plan is opt-in through
the resolver adapter; existing resolvers and default query planning remain
unchanged.
