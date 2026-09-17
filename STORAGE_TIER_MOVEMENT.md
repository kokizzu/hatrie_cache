# Storage Tier Movement

`StorageTierPolicy` can plan age-driven movement from hot, warm, and cold
tiers without changing the normal write path:

```go
parts := []hatStorage.StorageTierPart{
	{Key: "part-001", CurrentTier: "hot", Age: 2 * time.Hour},
}
moves, err := tiers.PlanStorageTierMoves(parts)
```

The planner returns only parts whose current tier differs from the policy's
age-selected tier. It preserves input order, selects source and destination
paths deterministically through the configured `DiskPlacementPolicy`, and
rejects empty keys, duplicate keys, unknown tiers, and negative ages.

Execution is explicit and resumable:

```go
report, err := hatStorage.ExecuteStorageTierMoves(
	ctx,
	tiers,
	parts,
	func(ctx context.Context, move hatStorage.StorageTierMove) error {
		// Copy to a destination temporary file, fsync, verify the checksum,
		// atomically publish, then remove the source and update metadata.
		return moveOne(ctx, move)
	},
)
```

`StorageTierMoveReport.Planned` is the total number of moves and `Moved` is
the number of callbacks that returned successfully. Cancellation or callback
failure stops at the next boundary and returns the partial count. The library
does not copy, rename, delete, retry, or reorder files; the callback must make
those operations idempotent and durable for the deployment's storage system.

There is no background mover and no default configuration change. This keeps
backup, restore, checksum, and cross-volume failure policy in the application
that owns the part format. Treat configured policy paths as trusted operator
configuration and keep callback path handling confined to those roots.
