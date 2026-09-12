# SQL Quotas

CH-029 adds an opt-in, caller-keyed rolling quota registry for SQL execution.
It is disabled by default: leave `SQLQueryOptions.Quota` nil to retain the
existing execution path.

## Configure

```go
registry, err := hatSql.NewSQLQuotaRegistry(hatSql.SQLQuotaRegistryOptions{
	Limits: hatSql.SQLQuotaLimits{
		MaxQueries:       100,
		MaxResultBytes:   10 << 20,
		MaxExecutionTime: 2 * time.Second,
		Window:           time.Minute,
	},
	MaxKeys: 4096,
})
if err != nil {
	return err
}

result, err := hatSql.ExecuteSQLQueryParameters(
	ctx,
	source,
	resolver,
	parameters,
	hatSql.SQLQueryOptions{
		Quota:    registry,
		QuotaKey: userID,
	},
)
```

`QuotaKey` can identify a user, tenant, API token, or other caller-owned
identity. An empty key uses the bounded `default` key. The same registry can
be shared by concurrent requests; each request supplies its own key.

All limit components are independent and zero disables that component:

| Option | Meaning |
| --- | --- |
| `MaxQueries` | Maximum admitted queries per key in the rolling window. Active reservations count against the limit. |
| `MaxResultBytes` | Cumulative materialized-result bytes or streamed-row bytes per key in the rolling window. |
| `MaxExecutionTime` | Cumulative wall-clock execution time per key in the rolling window. This is elapsed time, not process CPU time. |
| `Window` | Rolling window length. Zero defaults to one minute. |
| `MaxKeys` | Maximum number of distinct key states. Zero defaults to 4096. |

Use `ExecuteSQLQueryRows` to apply the same registry to streaming execution.
Rows already delivered to the callback cannot be retracted; callers must treat
`ErrSQLQuotaExceeded` as a failed stream and discard partial output. Materialized
execution returns the quota error without exposing the over-limit result rows.

`ErrSQLQuotaExceeded` identifies a query, byte, or elapsed-time limit.
`ErrSQLQuotaKeysExceeded` identifies a full registry. Both errors can be
checked with `errors.Is`.

## Memory and resolution

The registry uses 32 lock-sharded maps and at most 64 time buckets per active
key. The bucket width is `Window / 64` (with a one-nanosecond minimum), so
expiration is bounded by that resolution rather than requiring one event per
query. Expired inactive keys are removed when admission reaches the key bound.
No background cleanup goroutine is required.

For custom execution wrappers, `registry.Begin(key)` returns a
`SQLQuotaReservation`; call `Finish(resultBytes, elapsed)` exactly once. A
repeated finish is harmless. The `hatCache` package also exports aliases for
the registry, limits, options, reservation, and errors.

## Measured overhead

The focused benchmark uses one small `FROM VALUES` query and five benchmark
samples per case on Linux `amd64` (AMD Ryzen 9 5950X). The baseline leaves
`Quota` nil; the quota case enables one registry and one key.

| Case | Samples (ns/op) | Median ns/op | B/op | Allocs/op | Relative time | Relative bytes | Relative allocs |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Quota disabled | 7283, 6733, 6470, 5860, 6218 | 6470 | 10304 | 62 | 1.00x | 1.00x | 1.00x |
| Quota enabled | 9172, 8827, 8843, 8677, 8990 | 8843 | 11027 | 94 | 1.37x | 1.07x | 1.52x |

Run it with:

```text
make benchmark-ch029-sql-quotas
```

The overhead is paid only by calls that opt into a registry. The registry's
retained state is bounded by `MaxKeys * 64` buckets, independent of the number
of queries processed for an existing key.
