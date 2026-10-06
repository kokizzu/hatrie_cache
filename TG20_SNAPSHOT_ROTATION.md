# T-G20 Snapshot Rotation

T-G20 adds an opt-in cadence check and a non-destructive retention planner for
backup manifests. It is intentionally a policy API, not a background worker:
existing backup and restore behavior is unchanged until an application creates
and uses `hatBackup.SnapshotRotationPolicy`.

## Defaults

`SnapshotRotationOptions{}` is disabled. When `Enabled: true` and fields are
omitted, the policy uses:

- a one-hour snapshot cadence;
- the two newest complete backup chains;
- no byte limit.

The planner always retains the newest complete chain, even if that chain alone
exceeds `MaxBytes`; it sets `OverBudget` so an operator can decide whether to
increase the budget or perform another full backup.

## Example

```go
policy, err := hatBackup.NewSnapshotRotationPolicy(hatBackup.SnapshotRotationOptions{
    Enabled:    true,
    Cadence:    6 * time.Hour,
    KeepLatest: 3,
    MaxBytes:   20 << 30,
})
if err != nil {
    return err
}

if policy.ShouldSnapshot(lastSnapshotAt, time.Now()) {
    // Run the existing consistent snapshot/backup operation here.
}

plan, err := policy.Plan(manifests)
if err != nil {
    return err
}
// Review plan.DeleteBackupIDs, verify restore coverage, then delete payloads
// through the storage provider. Plan never deletes files or objects itself.
```

## Safety Contract

The planner validates manifest IDs, parent links, storage identity and
generation, monotonic journal sequences, file paths/checksums, and conflicting
object sizes. Incremental parents are retained with their selected tip, so
rotation cannot leave a restore chain without its base. `JournalSequence`
remains the WAL replay boundary; the planner does not rewrite or truncate the
journal.

The byte estimate uses `NewObjectBytes` when present, otherwise the sum of
manifest file sizes. It is an accounting estimate, not permission to delete
objects. Content-addressed object garbage collection must still use the
existing manifest/reference checks.

## Cost

The cadence check is allocation-free. On the benchmark host, the median was
13.75 ns/op with 0 B/op and 0 allocs/op. Planning a 64-manifest chain was
236.1 microseconds, 288.9 KB, and 590 allocations after removing redundant
chain cloning. The existing single-chain validator was 55.3 microseconds,
99.96 KB, and 209 allocations. This overhead is paid only when an operator
asks for a rotation plan; no write, read, or backup hot path calls it
automatically.

The benchmark target is:

```text
go test ./hat/hatBackup -run '^$' -bench 'TG20|SnapshotRotation' -benchmem -count=5
```
