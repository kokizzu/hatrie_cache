# M217 Materialized Point Lookup Index

## Decision

M217 adds an opt-in maintained point lookup index to `hatSql.MaterializedViews`.
`CreatePointLookupIndex` indexes complete rows from one materialized view using
a caller-supplied deterministic key function. Refreshes rebuild affected
indexes before publishing the new view snapshot, so view rows and point lookup
rows change atomically. Duplicate keys return all matching rows, and
`LookupPoint` returns cloned rows that callers may safely mutate. Dropping a
view also drops its point lookup indexes.

The index stores references to the immutable snapshot row maps plus key maps
and row slices; it does not duplicate complete row payloads. Exact arbitrary
keys remain caller-defined because dynamic SQL row values do not have one
universal lossless string encoding.

## Correctness

Commands:

```text
make test-m217-materialized-point-lookup
make race-m217-point-lookup
```

Both focused tests pass. Coverage includes create and refresh maintenance,
duplicate keys, result isolation, removed keys, empty snapshots, and atomic
preservation of the previous view/index when a refresh key function fails.

## Measurements

Five `-benchmem` samples ran on Linux amd64 on an AMD Ryzen 9 5950X with a
10,000-row materialized view. The point probe targets a row near the end of
the full snapshot.

| Workload | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Full snapshot clone and linear scan | 3,370,318 | 3,441,998 | 20,003 | baseline |
| Maintained point lookup | 404.4 | 376 | 4 | 8,334x faster; 9,154x lower bytes; 5,001x fewer allocations |

Refresh maintenance cost was measured on the same 10,000-row view:

| Refresh path | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing materialized refresh | 8,776,015 | 10,330,792 | 60,021 | baseline |
| Refresh with one maintained point index | 11,006,357 | 11,197,962 | 70,057 | 25.4% slower; 8.4% higher bytes; 16.7% more allocations |

Command:

```text
make benchmark-m217-point-lookup
```

The read win is substantial and the refresh cost is bounded and explicit, so
the feature is retained as opt-in rather than changing existing materialized
view defaults.
