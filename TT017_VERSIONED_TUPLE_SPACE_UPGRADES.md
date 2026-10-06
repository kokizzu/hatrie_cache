# TT-017 Versioned Tuple Space Upgrades

`hatDataStructure.VersionedTupleSpace` is an opt-in keyed space that can
upgrade records between registered `VersionedTupleMigrationManager` formats
without stopping the caller's service.

## Why

Schema migration normally has two bad choices: block all reads and writes
until every row is rewritten, or let old and new records remain unmanaged.
This controller supports a bounded middle path:

- `Restore` validates and preserves the version from a snapshot or recovery
  stream.
- `StartUpgrade` snapshots the older keys and fixes one target version.
- `UpgradeStep(limit)` performs at most `limit` conversions and returns status.
- `Get` lazily upgrades an old hot record.
- `Upsert` always writes the active upgrade target, so new old-version writes
  do not extend the queue.
- generation checks prevent a slower migration callback from overwriting a
  concurrent update or restore.

The controller has no background goroutine. The service owns scheduling,
batch size, observability, retries, and shutdown behavior.

## Example

```go
manager, err := hatDataStructure.NewVersionedTupleMigrationManager(formatV2)
if err != nil {
    return err
}
if err := manager.RegisterFormat(formatV1); err != nil {
    return err
}
if err := manager.RegisterMigration(1, 2, addActiveDefault); err != nil {
    return err
}

space, err := hatDataStructure.NewVersionedTupleSpace(manager)
if err != nil {
    return err
}

// Recovery keeps the snapshot's original version until the online upgrade.
if err := space.Restore("customer/42", oldTuple); err != nil {
    return err
}

status, err := space.StartUpgrade()
if err != nil {
    return err
}
for status.Active {
    status, err = space.UpgradeStep(128)
    if err != nil {
        // Failed records remain at their old version and are visible in
        // status.Failed/status.LastError for an explicit retry decision.
        return err
    }
}
```

`StartUpgrade` is a no-op completed status when every record already has the
manager's current version. A failed record is never partially published. A
later `StartUpgrade` can retry the remaining old records after the migration
catalog or source data has been repaired.

## Operational limits

Keys must be non-empty and no larger than `MaxVersionedTupleSpaceKeyBytes`.
Tuples are cloned at the storage boundary and when returned, so the space does
not retain caller-owned backing arrays. Migration callbacks run outside the
space lock, but they should still be deterministic and bounded because they
are part of the upgrade worker's latency budget.

The API does not provide durable storage, replication, distributed fencing, or
automatic scheduling. Persist the returned tuples through the existing storage
path and coordinate multiple processes at the application layer.

## Measurement

`make benchmark-tg17` compares the online controller with direct migration of
the same 1,024 old tuples on the same manager. Five samples were recorded on
an AMD Ryzen 9 5950X:

| Path | Median ns/op per 1,024 records | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Online `VersionedTupleSpace` | 1,515,545 | 1,386,929 | 13,333 |
| Direct `MigrateTo` control | 846,866 | 1,212,423 | 9,216 |
| Online/direct | 1.79x | 1.14x | 1.45x |

This is an operational feature with an intentional control-plane cost. It was
not enabled in existing tuple paths, and the benchmark is a transparent
comparison against the lower-overhead direct conversion loop.
