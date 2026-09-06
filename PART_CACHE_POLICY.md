# Part Cache Policy

`hat/hatStorage.PartCachePolicy` provides explicit admission and eviction
decisions for immutable storage parts. It does not own cached data or hidden
background state.

```go
policy, err := hatStorage.NewPartCachePolicy(1<<30, 2)
if err != nil {
	return err
}
	candidate := hatStorage.PartCacheCandidate{
		Key: "part-001", SizeBytes: 4096, Accesses: 3, LastAccess: 10,
	}
if policy.Admit(candidate) {
	// Store the part in the caller's cache.
}
evictions, err := policy.PlanEvictions(existing, candidate.SizeBytes)
```

Admission requires a non-empty key, a non-zero size that fits within
`CapacityBytes`, and at least `MinAccesses` observations. `PlanEvictions`
validates unique candidates and returns the smallest prefix that frees enough
space for the incoming part. Its deterministic order is least `Accesses`,
oldest `LastAccess`, largest `SizeBytes`, then lexical key.

The policy is useful for part-level cache admission without forcing a specific
cache implementation. Callers apply the returned plan and remain responsible
for tracking access counters, updating byte usage, and handling concurrent
cache mutations.
