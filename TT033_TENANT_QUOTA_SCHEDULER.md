# TT-033 Tenant Quota Scheduler

`hatPipeline.TenantQuotaScheduler` is an opt-in wrapper around the existing
bounded `hatPipeline.Scheduler`. It limits each tenant independently so one
tenant cannot consume every active task slot or create an unbounded waiting
queue.

## Usage

```go
ctx := context.Background()
scheduler, err := hatPipeline.NewTenantQuotaScheduler(hatPipeline.TenantQuotaSchedulerOptions{
	Workers:       8,
	QueueCapacity: 64,
	DefaultPolicy: hatPipeline.TenantQuotaPolicy{MaxConcurrent: 2, MaxQueued: 16},
})
if err != nil {
	return err
}

if err := scheduler.Submit(ctx, "region-apac", func(ctx context.Context) error {
	return processTenantWork(ctx, "region-apac")
}); err != nil {
	return err
}

return scheduler.Wait()
```

Use `PolicyFor` when tenants need different limits. The callback is evaluated
when a tenant becomes active; the selected policy remains stable until that
tenant has no active or waiting work.

```go
PolicyFor: func(tenant string) hatPipeline.TenantQuotaPolicy {
	if tenant == "premium" {
		return hatPipeline.TenantQuotaPolicy{MaxConcurrent: 4, MaxQueued: 64}
	}
	return hatPipeline.TenantQuotaPolicy{MaxConcurrent: 1, MaxQueued: 4}
},
```

## Defaults And Semantics

- `MaxConcurrent: 0` selects one active task per tenant.
- `MaxQueued: 0` is valid and rejects work while all tenant slots are busy.
- The global `Workers` and `QueueCapacity` limits remain in force.
- A full tenant waiting budget returns `ErrTenantQuotaQueueFull` immediately.
- A canceled caller context cancels admission while it is waiting for a tenant
  slot or global queue capacity.
- `Close` rejects new work and drains accepted work. `Cancel` stops scheduler
  work; call `Wait` to join workers and clear quota state for discarded tasks.
- Tenant identifiers are trimmed and empty identifiers are rejected.

The quota scheduler does not change `NewScheduler`, and it does not become a
default for existing callers. The task callback receives the scheduler context
following the existing `Scheduler` contract after admission succeeds.

## Measurement

Environment: Linux amd64, AMD Ryzen 9 5950X 16-Core Processor. Command:
`make round68-tt033-bench`. Each sample creates a scheduler, submits one no-op
task, and waits for completion. This measures lifecycle overhead, not a
steady-state application workload.

| Variant | Samples (ns/op) | Median | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| `Scheduler` | 1218, 1232, 1278, 1291, 1227 | 1232 | 600 | 9 |
| `TenantQuotaScheduler` | 2061, 2089, 2135, 2086, 2202 | 2089 | 1448 | 18 |

The opt-in wrapper is `1.70x` slower for this lifecycle, uses `2.41x` the
bytes, and uses `2x` the allocations. That is an intentional cost for
per-tenant fairness, bounded overload admission, and cancellation cleanup; it
is not a raw-throughput optimization. Use the existing `Scheduler` when those
controls are unnecessary.
