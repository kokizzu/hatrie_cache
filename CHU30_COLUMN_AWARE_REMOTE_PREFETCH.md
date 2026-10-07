# CH-U30 Column-Aware Remote Prefetch

CH-U30 adds an explicit read-ahead policy on top of the existing bounded
`hatStorage.RemotePartCache`. A caller supplies independently readable remote
objects for a part's columns and names the columns needed by the upcoming
read. The policy selects requested columns in deterministic order, skips
columns that would exceed `MaxBytes`, and sends only selected references to
the existing single-flight `Prefetch` implementation.

## API

```go
columns := []hatStorage.RemotePartColumnReference{
	{Name: "id", Reference: idReference},
	{Name: "body", Reference: bodyReference},
}
options := hatStorage.RemotePartColumnPrefetchOptions{
	MaxBytes:      64 << 20,
	MaxConcurrent: 2,
	Priority:      5,
}
plan, err := cache.PrefetchColumns(ctx, columns, []string{"id"}, options, loader)
```

`PlanRemotePartColumnPrefetch` is available when the caller wants to inspect
the decision before loading. The plan reports selected/skipped column names,
references, and byte totals. Missing, duplicate, empty, or invalid column
references fail closed. A zero `MaxBytes` means no extra policy budget; the
cache's own byte and entry limits still apply.

Skipped columns are not errors. They can be loaded later through the existing
on-demand cache methods, so prefetch remains an optimization rather than a
data-availability requirement. The loader contract is unchanged and receives
the selected `RemotePartReference`; the caller maps each reference to its
column object.

## Defaults and safety

The feature is opt-in. Existing `Get`, `Acquire`, and `Prefetch` calls do not
inspect column metadata or pay for column planning. No cache key, eviction
rule, checksum validation, or loader behavior changed. The caller remains
responsible for the remote manifest, column-to-object mapping, and fallback
on-demand reads.

## Measurement

The clean-base existing bounded-2 prefetch benchmark, five runs, measured
**70.635-75.290 us/op, 142,381-142,388 B/op, and 100 allocs/op** for 16
4 KiB objects. The feature branch's all-column control measured
**60.003-63.377 us/op** with **142,384-142,396 B/op and 100 allocs/op**.

The selected-column case requested 4 of the 16 objects and capped the policy
at 16 KiB. Five runs measured **23.631-28.430 us/op, 38,815-38,819 B/op,
and 37 allocs/op**. The selected case therefore reduced the requested wire
bytes from 64 KiB to 16 KiB (4.0x), reduced benchmark allocation bytes by
about 3.67x, reduced allocations by about 2.70x, and reduced mean elapsed
time from about 61.78 us to 26.25 us (about 2.35x) for this workload. These
are workload reductions from reading fewer independent column objects, not a
claim that the same bytes transfer faster.

The benchmark command was:

```text
make codex-chu30-bench-feature
```

Focused tests, race tests, and vet passed. The combined `hatStorage` and
`hatSql` check remains blocked by the pre-existing M-U05 arrangement-recovery
failures in `hat/hatSql`; this feature's package tests pass.
