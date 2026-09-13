# SQL Namespace Admission Profiles

`hatSql` supports optional ClickHouse-style soft and hard resource profiles for
namespace-governed SQL. Hard limits retain the existing behavior: they tighten
the `SQLQueryOptions` passed to execution. Soft limits are advisory and are
reported to operators before a query is submitted.

## Configure

```go
governor, err := hatSql.NewNamespaceQueryGovernorWithProfiles(
	hatSql.NamespaceResourceProfile{
		Soft: hatSql.NamespaceResourceLimits{
			MaxRows:      1_000,
			MaxJoinBytes: 1 << 20,
		},
		Hard: hatSql.NamespaceResourceLimits{
			MaxRows:      10_000,
			MaxJoinBytes: 16 << 20,
		},
	},
	map[string]hatSql.NamespaceResourceProfile{
		"tenant-a": {
			Soft: hatSql.NamespaceResourceLimits{MaxRows: 500},
			Hard: hatSql.NamespaceResourceLimits{MaxRows: 5_000},
		},
	},
)
if err != nil {
	return err
}
defer governor.Close()
```

`NewNamespaceQueryGovernor` remains the compatibility constructor for hard-only
policies. In a profile map, zero fields inherit the corresponding default
profile. A configured soft limit above its effective hard limit is capped at
the hard limit. Negative values and existing governor bounds are rejected.

## Preview

```go
admission, err := governor.DryRun("tenant-a", hatSql.SQLQueryOptions{
	MaxRows:      20_000,
	MaxJoinBytes: 8 << 20,
})
if err != nil {
	return err
}

// EffectiveOptions is what Execute will use for the same namespace.
log.Printf("hard caps=%v soft warnings=%v", admission.HardClamps, admission.SoftWarnings)
```

`HardClamps` and `SoftWarnings` contain stable option names such as
`max_rows`, `max_join_bytes`, `workers`, and `timeout`. `HardLimits` and
`SoftLimits` expose the effective static policies. The result is caller-owned.

`DryRun` is deliberately static. It does not reserve a concurrency slot,
consume a rate quota, enqueue work, start a compute worker, or execute SQL.
Use it for admission explanations, configuration validation, and dashboards;
call `Execute` for the authoritative runtime queue/quota decision.

## Cost

The existing `Execute` path still applies only hard limits and does not call
`DryRun`, so its default runtime behavior is unchanged. The optional preview
allocates only when it returns non-empty clamp or warning lists. See the
[CH-003 benchmark](BENCHMARK.md#ch-003-soft-and-hard-namespace-admission-profiles)
for raw before/after measurements.
