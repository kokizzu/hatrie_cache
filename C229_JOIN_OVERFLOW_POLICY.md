# C229 Join Overflow Policy

`SQLQueryOptions.JoinOverflowPolicy` makes the resource disposition of a
configured `MaxJoinBytes` budget explicit.

```go
options := hatSql.QueryOptions{
	JoinOverflowPolicy: hatSql.SQLJoinOverflowSpill,
	MaxJoinBytes:       8 << 20,
	SpillDirectory:     "/var/lib/hatrie/spill",
	MaxSpillBytes:      1 << 30,
}
```

## Policies

| Policy | Behavior |
| --- | --- |
| empty / `SQLJoinOverflowAuto` | Preserves the existing behavior. An eligible direct equality join uses configured spill settings; other joins use their existing executor. |
| `SQLJoinOverflowReject` | Keeps the materialized path. When `MaxJoinBytes` is positive, a materialized join input over that encoded-byte budget is rejected before the join is built. No spill files are created. |
| `SQLJoinOverflowSpill` | Requires positive `MaxJoinBytes`, `SpillDirectory`, and `MaxSpillBytes`, plus a direct two-source `CACHE` `INNER` equality join. Unsupported shapes fail instead of silently falling back to an unbounded materialized join. Temporary files are removed before return. |

The policy is validated for every query entry point. Unknown values, including
`"truncate"`, fail validation. Truncating a join would return an incomplete
SQL result and is therefore intentionally not supported. Use SQL `LIMIT` or
`ExecuteSQLQueryPage` when bounded output is needed; those APIs preserve an
explicit result contract.

`SpillBloom` can optionally skip partition pairs whose compact Bloom filters
cannot intersect. `MaxSpillBytes` remains the hard temporary-disk budget, and
the spill path still honors context cancellation and join-work limits.

The default remains zero-value `auto`; existing callers do not pay for or opt
into a new overflow policy. See [BENCHMARK.md](BENCHMARK.md#c229-join-overflow-policy)
for CPU, heap, allocation, and raw benchmark results.
