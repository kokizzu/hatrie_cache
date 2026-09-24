# TT-033 Fiber Scheduler Quotas

`hatFiber` now supports optional per-tenant limits for cooperative fibers.
The feature is opt-in. Existing `Spawn` and `Run` callers keep the ordinary
zero-allocation scheduler path when no step quotas are configured.

## API

```go
scheduler, err := hatFiber.New(hatFiber.Options{
	MaxFibers: 1024,
	TenantQuotas: map[string]hatFiber.TenantQuota{
		"api": {
			MaxFibers:      256,
			MaxStepsPerRun: 1024,
		},
	},
})
if err != nil {
	return err
}

id, err := scheduler.SpawnForTenant("api", step)
if err != nil {
	return err
}

stats, err := scheduler.Run(ctx, 0)
_ = id
_ = stats
```

`TenantQuota.MaxFibers` limits retained fibers for that tenant. A terminal
fiber continues to count until `Reap` releases its slot, which prevents a
caller from bypassing the limit by spawning faster than it reaps. A value of
zero means unlimited.

`TenantQuota.MaxStepsPerRun` limits callbacks for that tenant during one
`Run` call. A value of zero means unlimited. When a tenant reaches its budget,
its ready fibers remain queued for a later `Run`; `RunStats.Throttled` records
how many ready callbacks were skipped for that reason. The scheduler is
cooperative: a callback that does not return cannot be stopped by this quota.

Unconfigured tenant names are unlimited. The unscoped `Spawn` method is also
unlimited and retains the pre-feature behavior. Invalid empty configured names
and negative fiber limits return configuration errors from `New`.

## Measured Cost

Command:

```text
make benchmark-tt033-tenant-quota
```

The benchmark uses five samples on an AMD Ryzen 9 5950X and exercises a
64-fiber batch. The baseline was recorded before the quota implementation;
the other rows were measured after the fast-path split.

| Path | Median ns/op | B/op | allocs/op | Relative to pre-change default |
| --- | ---: | ---: | ---: | ---: |
| Default scheduler, before TT-033 | 1,152 | 0 | 0 | 1.00x |
| Default scheduler, after TT-033 | 1,170 | 0 | 0 | 1.02x |
| `SpawnForTenant`, fiber quota only | 6,185 | 6,400 | 8 | 5.37x |
| `SpawnForTenant`, fiber + step quota | 7,875 | 6,400 | 8 | 6.84x |

The default difference is within normal benchmark noise while preserving zero
allocations. The configured rows include scheduler creation, tenant quota
maps, 64 admissions, one `Run`, and 64 reaps. Step accounting intentionally
allocates a per-run tenant counter map; use only the fiber quota when admission
control is needed without per-run accounting overhead.

## Correctness Coverage

The focused tests cover quota rejection and reuse after `Reap`, cancellation
requiring `Reap`, round-robin progress for other tenants while one tenant is
throttled, and invalid configuration. The package test, race test, and vet
commands are available through `make verify-tt033-tenant-quota`.
