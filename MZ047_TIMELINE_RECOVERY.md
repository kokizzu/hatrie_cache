# MZ047 Timeline Recovery

MZ047 adds a small, opt-in reconciliation boundary for crash recovery and
partially persisted multi-source cutovers. Existing `hatPipeline` snapshots
restore a frontier registry or connector registry independently. This planner
compares those persisted checkpoints with the progress observed after restart,
so a caller can avoid silently publishing a mixed timeline.

## Usage

```go
persisted := []hatPipeline.TimelineRecoveryCheckpoint{
	{ID: "orders", Lower: 100, Upper: 120, Generation: 8},
}
observed := []hatPipeline.TimelineRecoveryCheckpoint{
	{ID: "orders", Lower: 96, Upper: 116, Generation: 7},
}

plan, err := hatPipeline.ReconcileTimelineRecovery(
	persisted,
	observed,
	hatPipeline.TimelineRecoveryOptions{},
)
if err != nil {
	return err
}
for _, decision := range plan.Decisions {
	// Replay decision.ReplayFrom through decision.ReplayThrough before
	// publishing the component's recovered frontier.
	_ = decision
}
if plan.HasQuarantine() {
	// Stop publication and require an operator or a higher-level recovery
	// protocol to resolve progress that is newer than the persisted state.
}
```

The returned decisions are sorted by component ID and do not alias either
input slice. The planner performs no I/O and does not mutate a registry.

## Actions

| Action | Condition | Caller behavior |
| --- | --- | --- |
| `adopt` | Lower, upper, and generation exactly match | Use the observed checkpoint as-is. |
| `replay` | Every observed value is at or behind the persisted value, and at least one is older | Replay from `Observed.Lower` to `Persisted.Lower`; generation rollback is also replay. |
| `quarantine` | Any observed value is ahead, or progress is mixed (for example an older lower bound with a newer upper bound) | Do not publish silently; resolve or discard the newer state explicitly. |

`SafeLower` and `SafeUpper` are the minimum lower and upper bounds across both
checkpoint sets. They are conservative publication limits while the caller
executes the plan.

Both input sets must contain the same unique non-empty IDs, with
`Lower <= Upper`. `MaxComponents` defaults to
`DefaultTimelineRecoveryMaxComponents` (1024) and is bounded to prevent an
untrusted recovery request from allocating without limit. A zero-component
checkpoint is allowed and produces an empty plan; a one-sided checkpoint is a
component mismatch.

## Cost And Measurement

The benchmark uses 128 exact components on an AMD Ryzen 9 5950X and compares
the planner with a manual caller-side linear scan. The planner is a recovery
control-plane operation, not a request-path hook.

| Implementation | Median ns/op | B/op | allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Manual linear reconciliation | 53,506 | 16,584 | 4 | 1.00x |
| MZ047 typed-slice reconciliation | 30,699 | 27,184 | 5 | 1.74x faster |

An earlier map-based implementation was rejected during the same change: it
used about 39,000 to 46,700 ns/op, 51,536 B/op, and 8 allocations. Sorting two
detached typed slices removed the second map and reduced retained planning
memory by about 47%, while improving median time by about 1.74x against the
manual baseline. The remaining one-allocation and 10,600-byte cost over the
manual path buys validation, deterministic ordering, detached output, and
explicit quarantine semantics; it is paid only when recovery is requested.

Raw final samples (`ns/op`, `B/op`, `allocs/op`):

```text
MZ047: 30076 27184 5
MZ047: 30699 27184 5
MZ047: 30414 27184 5
MZ047: 31898 27184 5
MZ047: 31489 27184 5
manual: 53715 16584 4
manual: 53506 16584 4
manual: 51835 16584 4
manual: 53632 16584 4
manual: 52707 16584 4
```

Run the benchmark with:

```text
make benchmark-mz047-timeline-recovery
```

## Verification

The focused tests cover adopt, replay, quarantine, mixed progress, duplicate
IDs, mismatched component sets, invalid bounds, limits, deterministic order,
and detached results. The feature uses the existing repository Makefile
targets:

```text
make test-mz047-timeline-recovery
make test-mz047-package
make race-mz047-timeline-recovery
make vet-mz047-timeline-recovery
```
