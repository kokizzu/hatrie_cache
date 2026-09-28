# M246 History Retention Policy

M246 adds an optional per-frontier policy for historical reads. It combines a
logical age bound with caller-reported retained-history bytes, so a compactor
or query coordinator can reject work that would require more history than the
frontier is configured to keep.

## API

```go
frontiers, _ := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{})
_ = frontiers.Register("orders")
_ = frontiers.Advance("orders", 10, 100)

retention, _ := hatPipeline.NewFrontierRetentionRegistry(
	frontiers,
	hatPipeline.FrontierRetentionOptions{},
)
_ = retention.SetPolicy("orders", hatPipeline.FrontierRetentionPolicy{
	MaxAge:   10,   // logical timestamp units
	MaxBytes: 1 << 20,
})
_ = retention.ObserveStorage("orders", 512<<10)

lease, err := retention.Acquire("orders", 95) // accepted: age is 5
if err == nil {
	defer retention.Release(lease)
}
_, err = retention.Acquire("orders", 80) // ErrFrontierRetentionPolicyViolation
```

`MaxAge == 0` disables the age bound. `MaxBytes == 0` disables the storage
bound. The age check compares the requested `asOf` timestamp with the current
frontier upper bound. The boundary itself is accepted: a request is rejected
only when `upper - asOf > MaxAge`.

`ObserveStorage` is an atomic caller-side accounting update. An update above
`MaxBytes` is rejected and the previous value remains visible. The registry
does not inspect or measure the backing store, and it does not compact data;
storage owners should report their retained bytes after writes and compaction.

`Snapshot` exposes the active policy and last accepted byte count. `ClearPolicy`
removes both limits while retaining the last byte count for observability. A
zero policy and zero observed bytes release all optional policy bookkeeping.

## Default and cost

Policies are disabled by default. The normal lease path keeps its original
state layout and does not allocate the optional policy map. Configured
frontiers retain one small policy marker so repeated acquire/release cycles do
not recreate policy state.

The focused benchmark compares the unchanged lease path against `origin/master`
and measures the configured path separately:

| Path | Median time | Memory/op | Allocs/op | Meaning |
| --- | ---: | ---: | ---: | --- |
| Origin legacy acquire/release | 412 ns/op | 424 B | 3 | Baseline |
| M246 legacy acquire/release | 417 ns/op | 424 B | 3 | Policy disabled |
| M246 policy acquire/release | 213 ns/op | 0 B | 0 | Policy enabled, marker retained |

The policy-enabled result is not a claim that policy checks are intrinsically
faster. It benefits from retaining the per-frontier lease map between cycles;
the baseline path drops that map after every release. The important default
compatibility result is unchanged allocation volume and benchmark noise-level
CPU difference. Re-run `make benchmark-m246` and
`make benchmark-m246-before-after` on the target machine before using these
numbers for capacity planning.

## Correctness contract

- Existing leases remain valid when a stricter policy is installed; new
  acquisitions are checked against the new policy.
- A rejected age or byte update does not mutate the lease or byte accounting.
- Policies are independent by frontier ID.
- Existing frontier lower/upper checks, compaction safety, close behavior, and
  lease limits remain unchanged.
