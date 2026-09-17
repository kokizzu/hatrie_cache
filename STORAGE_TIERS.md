# Storage Tiers

`hat/hatStorage.StorageTierPolicy` classifies a part by age and delegates
physical path selection to a `DiskPlacementPolicy`. The policy is immutable
after construction and can be configured in any order.

```go
hot, _ := hatStorage.NewDiskPlacementPolicy("hot", []hatStorage.DiskPlacementRule{
	{Path: "/data/hot", Weight: 1},
})
warm, _ := hatStorage.NewDiskPlacementPolicy("warm", []hatStorage.DiskPlacementRule{
	{Path: "/data/warm", Weight: 1},
})

tiers, err := hatStorage.NewStorageTierPolicy([]hatStorage.StorageTierRule{
	{Name: "warm", MinAge: time.Hour, Placement: warm},
	{Name: "hot", MinAge: 0, Placement: hot},
})
selection, err := tiers.Select(2*time.Hour, "part-001")
```

The selected rule is the one with the greatest `MinAge` not greater than the
part age. A zero-age rule is required. Names and thresholds must be unique,
placements must contain at least one path, and negative ages are unavailable.
`Rules` returns an independent normalized, age-sorted copy.

This API only decides policy. It does not monitor disk health or reclaim old
tiers. `PlanStorageTierMoves` adds a deterministic, mutation-free plan for
existing parts, and `ExecuteStorageTierMoves` invokes a caller-owned callback
for each planned move. Neither function starts background work or performs
file I/O. See [STORAGE_TIER_MOVEMENT.md](STORAGE_TIER_MOVEMENT.md) for the
operational contract.
