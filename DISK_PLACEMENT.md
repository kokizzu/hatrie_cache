# Disk Placement

`hat/hatStorage` exposes `DiskPlacementPolicy` for deterministic placement of
storage parts across multiple configured paths.

```go
policy, err := hatStorage.NewDiskPlacementPolicy("hot-warm", []hatStorage.DiskPlacementRule{
	{Path: "/data/hot", Weight: 1},
	{Path: "/data/warm", Weight: 3},
})
if err != nil {
	return err
}

path, err := policy.SelectPath("part-001")
```

The constructor copies the rules, rejects empty names or paths, zero weights,
duplicate paths, and total-weight overflow. `Rules` also returns a copy, so a
constructed policy cannot be changed through the returned slice. The same key
and policy always produce the same configured path. Selection hashes the key
once and walks the small weighted rule list; the hot path does not allocate.

This is a placement policy, not a disk health detector, migration engine, or
replication mechanism. Callers should verify that the selected path is usable
and decide how to recover when no rules are configured or a path fails.
