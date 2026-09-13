# MZ-027 Read-Hold Lifecycle Diagnostics

Materialize-style read holds are already enforced by
`hatPipeline.FrontierRetentionRegistry`: an acquired lease keeps historical
data available until it is released. This addition exposes the active handles
so operators can identify which frontier and timestamp are retaining history.

## API

`FrontierRetentionRegistry.ActiveLeases(frontierID)` returns a detached copy of
the currently active `FrontierRetentionLease` values for one registered
frontier. Results are sorted by lease ID, so repeated diagnostic reads are
deterministic. A registered frontier with no active leases returns a nil slice
and nil error.

The returned values can be modified by the caller. They do not change the
registry, acquire another hold, or extend an existing hold. The method returns
the existing frontier, empty-ID, and closed-registry errors for invalid input
and lifecycle state. A lease ID is the stable handle for correlating an
operator observation with the owner that created it; the registry does not
invent client identity or retain extra owner metadata.

## Example

```go
frontiers, _ := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{})
defer frontiers.Close()
frontiers.Register("events")
frontiers.Advance("events", 100, 130)

retention, _ := hatPipeline.NewFrontierRetentionRegistry(
	frontiers,
	hatPipeline.FrontierRetentionOptions{},
)
defer retention.Close()
lease, _ := retention.Acquire("events", 100)
defer retention.Release(lease)

leases, _ := retention.ActiveLeases("events")
fmt.Println(leases[0].ID, leases[0].AsOf)
```

Output:

```text
1 100
```

For a compaction dashboard, combine `ActiveLeases` with `Snapshot`:
`Snapshot` reports the safe compaction boundary and debt, while
`ActiveLeases` identifies the exact handles contributing to that debt.

## Cost

`ActiveLeases` is intentionally an on-demand diagnostic operation. It copies
and sorts the active values, so its cost grows with the number of active holds;
the normal acquire, release, and compaction-boundary paths do not perform this
copy. With 64 active holds, the five-sample median was about 5.22 microseconds,
2,408 B/op, and 4 allocs/op. The existing summary `Snapshot` was about 50 ns,
0 B/op, and 0 allocs/op under the same setup.

With no holds, the optimized path was about 34 ns, 0 B/op, and 0 allocs/op.
The pre-optimization empty path was about 65 ns and 24 B/op, so returning nil
for an empty diagnostic result removed that allocation and cut the empty call
cost by about 1.9x.

Reproduce with:

```text
make benchmark-mz027
```

