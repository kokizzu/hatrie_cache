# Cross-Region Replication Policy

`hat/hatCache.ReplicationRegionPolicy` records operator expectations for
cross-region replication without changing the existing replication defaults.
Configure the local region, required remote regions, a maximum journal-sequence
lag (an RPO proxy), and an optional recovery-time budget (RTO).

```go
policy := hatCache.ReplicationRegionPolicy{
	LocalRegion:           "asia",
	RequiredRemoteRegions: []string{"europe"},
	MaxRPOLagSequences:    10,
	MaxRTO:                30 * time.Second,
}
if err := policy.Validate(); err != nil {
	return err
}
```

`HTTPReplicator.RegionReplicationStatus` reports available and missing remote
regions, targets grouped by region, current maximum sequence lag, whether the
RPO budget is satisfied, and the configured RTO in milliseconds. Region names
are normalized and output is deterministic.

The policy is disabled when no regional fields are configured. It is an
observability and admission-policy building block: it does not automatically
route traffic, fail over, or claim that sequence lag is elapsed-time RPO. Use
it with the existing topology, fencing, and operational recovery procedures.
Topology region metadata is preserved through JSON and native gRPC.
