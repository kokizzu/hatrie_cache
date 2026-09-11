# SQL GROUP BY Key Limit

`SQLQueryOptions.MaxGroupKeys` is an opt-in guard for high-cardinality
aggregation. A positive value rejects a query before it retains the next
distinct `GROUP BY` key beyond that limit:

```go
result, err := hatSql.ExecuteSQLQueryContext(
	ctx,
	"FROM CACHE('events') SELECT region, COUNT(*) GROUP BY region",
	resolver,
	hatSql.SQLQueryOptions{MaxGroupKeys: 50_000},
)
```

The error contains `SQL group key limit exceeded`. A negative value is
invalid. `0` is the default and preserves the previous unbounded behavior.

Namespace policy can apply the same cap to every query in a namespace:

```go
limits := hatSql.NamespaceResourceLimits{MaxGroupKeys: 50_000}
options := limits.Apply(hatSql.SQLQueryOptions{})
```

The namespace limit only tightens a caller-provided positive value; it cannot
turn a stricter caller limit off.

`MaxGroupKeys` is independent of `MaxGroupRowsPerKey`, `MaxGroupBytes`, and
`MaxSpillBytes`. Grouped execution paths that cannot enforce the key cap
directly fall back to the bounded materialized implementation. This keeps the
limit effective when columnar, hash, dictionary, indexed-stream, or spill
optimizations would otherwise be eligible. Grouping-set branches are checked
independently. The result cache is bypassed when the option is non-zero.

The default path has no new retained state, storage format, wire format, or
configuration requirement. The guard is intended for tenant or workload
admission policy where a single query must not retain an unbounded number of
aggregation states.

## Measurement

The focused benchmark grouped 4,096 in-memory rows into 256 keys on an AMD
Ryzen 9 5950X with seven samples per case. The baseline is the repository
`HEAD` implementation before this feature. The guarded case enables
`MaxGroupKeys` while staying below the limit, so it measures the control-path
cost rather than a rejected query.

| Case | Median ns/op | Median B/op | Median allocs/op |
|---|---:|---:|---:|
| Baseline `HEAD` | 1,248,090 | 1,448,517 | 9,494 |
| Current, default `MaxGroupKeys: 0` | 1,240,472 | 1,448,520 | 9,494 |
| Current, guarded positive limit | 1,242,718 | 1,448,515 | 9,494 |

The current default was 1.01x faster in this run with identical allocation
count and effectively identical bytes. The guarded path was 0.18% slower than
the current default, with no additional allocations or retained state. This
is a bounded safety control, not a claimed general query-speedup.
