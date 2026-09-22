# M246 Per-Object History-Retention Policies

`hatPipeline.FrontierRetentionRegistry` now supports optional retention
budgets keyed by frontier ID. A frontier ID is the object boundary: callers
can give each table, stream, or materialized object its own logical-history
and estimated-byte limits.

The feature is caller-driven accounting. It does not delete history, start a
worker, or override lease/frontier safety. A compactor must still use
`SafeCompactionBefore` before removing history.

## API

```go
retention, err := hatPipeline.NewFrontierRetentionRegistry(
	frontiers,
	hatPipeline.FrontierRetentionOptions{MaxPolicies: 1024},
)
if err != nil {
	return err
}
defer retention.Close()

err = retention.SetPolicy("orders", hatPipeline.FrontierRetentionPolicy{
	MaxHistory: 100_000,  // logical timestamp units; zero disables this bound
	MaxBytes:   64 << 20, // caller-estimated retained bytes; zero disables it
})
if err != nil {
	return err
}

err = retention.SetUsage("orders", hatPipeline.FrontierRetentionUsage{
	RetainedHistory: 102_400,
	RetainedBytes:   70 << 20,
})
if err != nil {
	return err
}

policy, err := retention.PolicySnapshot("orders")
if err != nil {
	return err
}
if policy.BudgetExceeded {
	safe, err := retention.SafeCompactionBefore("orders")
	if err != nil {
		return err
	}
	// Pass safe to the compactor. The policy is a signal, not permission
	// to compact beyond the frontier/lease boundary.
	_ = safe
}
```

`SetPolicy` replaces an existing policy without consuming another policy
slot. `SetUsage` replaces the current caller-reported values and updates an
existing map entry; it does not allocate per update. `PolicySnapshots` returns
configured policies sorted by frontier ID. `ClearPolicy` removes a policy and
releases its bounded slot.

`MaxPolicies` defaults to `DefaultFrontierRetentionMaxPolicies` (`1024`) and
is hard-bounded at `1 << 20`. The policy map is lazy: a registry that never
calls `SetPolicy` does not allocate it. At least one of `MaxHistory` or
`MaxBytes` must be non-zero. These limits bound the accounting registry, not
the underlying data structure; `MaxBytes` is an estimate supplied by the
owner of that data structure.

The existing `FrontierRetentionSnapshot` and `Snapshot` path intentionally do
not contain policy fields. This preserves the hot monitoring path and keeps
legacy compaction metrics independent from optional policy accounting.

## Operational Workflow

1. Register a frontier for each object.
2. Configure a policy only for objects that need a budget.
3. Report usage after the object's own compaction/accounting pass.
4. Read `PolicySnapshot` or `PolicySnapshots` for alerting and scheduling.
5. When compacting, cap the request with `SafeCompactionBefore`; leases and
   frontier progress remain authoritative.
6. Clear policies for deleted objects so their bounded slots and bookkeeping
   are released.

No default retention policy is installed, and no automatic compaction is
enabled. This avoids changing existing storage lifetime or backup behavior.

## Tradeoff

The policy feature adds one bounded map entry per configured object and two
`uint64` usage values. The default registry path has no policy-map allocation.
The measured current no-policy `Snapshot` path is allocation-free and matched
the M245 parent within benchmark noise after policy state was moved out of the
existing snapshot result. See [BENCHMARK.md](BENCHMARK.md#m246-per-object-history-retention-policies).
