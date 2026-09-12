# T-G22 Incremental Backup Chain Planning

`hat/hatBackup` now validates and plans content-addressed incremental backup
chains before an operator prunes or transfers repository metadata. This is a
read-only metadata operation; it does not change backup creation, retention
defaults, or the write path.

## API

```go
chain, err := hatBackup.PlanBackupChain(manifests, latestBackupID)
if err != nil {
	return err
}
fmt.Println(chain.BaseBackupID, chain.LatestBackupID)

retention, err := hatBackup.PlanBackupRetention(manifests, latestBackupID, 32)
if err != nil {
	return err
}
// Delete only the IDs and object hashes returned by this reviewed plan.
```

`PlanBackupChain` accepts manifests in any order and returns independent
manifests ordered from the full base to `latestBackupID`. It verifies:

- every manifest is the supported incremental Pebble format;
- the base is not marked incremental and every child is marked incremental;
- every parent exists exactly once and the chain has no cycle;
- storage backend, format, identity, and generation stay constant;
- journal sequences never move backwards;
- file paths are relative and canonical, sizes are non-negative, SHA-256
  values are lowercase hexadecimal, and file paths are unique per manifest;
- the same content hash never claims different sizes.

`PlanBackupRetention` keeps the newest `retain` manifests and lists all other
manifest IDs as deletion candidates. It separately computes reachable object
hashes, so an object referenced by any kept manifest is never a deletion
candidate. It rejects mixed storage generations or identities. The planner is
deliberately non-mutating: a caller must review and apply the returned plan.

## Measured Cost

Measured on Linux amd64 with an AMD Ryzen 9 5950X, Go benchmark mode, five
runs, and a synthetic 128-manifest chain:

| Operation | Time | Allocated memory | Allocations |
| --- | ---: | ---: | ---: |
| `PlanBackupChain` | 75-87 us/op | 147,640-147,641 B/op | 397/op |
| `PlanBackupRetention` | 97-101 us/op | 158,209 B/op | 429/op |

The benchmark command is:

```text
make benchmark-t-g22-backup-chain
```

There was no previous chain planner to compare against. The relevant tradeoff
is therefore new operator validation cost versus avoiding an unsafe prune or a
restore that discovers a broken parent/object relationship too late. The
planner is not called by normal reads, writes, backup creation, or restore.

## Failure Handling

The planner returns before any filesystem mutation. A backup tool can use the
returned plan as a review checkpoint, verify the repository still matches the
manifest set, and then remove only the listed files. If validation fails, keep
the repository unchanged and repair or restore the missing manifest/object
first.
