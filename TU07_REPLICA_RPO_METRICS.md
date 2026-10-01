# T-U07 Replica RPO Metrics

`hatCache` exposes regional replication health through Prometheus only when
`HTTPReplicatorOptions.ReplicationRegionPolicy` is configured. The zero-value
policy keeps the metrics off and preserves the existing monitoring path.

The exported policy fields are:

- `LocalRegion`: this node's region.
- `RequiredRemoteRegions`: regions that must be represented by remote topology
  nodes.
- `MaxRPOLagSequences`: maximum allowed source-sequence lag; zero disables the
  lag limit.
- `MaxRTO`: recovery-time budget; zero disables the RTO limit.

The metrics use only the `node` label to avoid region and target cardinality:

- `hatrie_cache_replication_rpo_configured`
- `hatrie_cache_replication_rpo_configuration_error`
- `hatrie_cache_replication_rpo_within_budget`
- `hatrie_cache_replication_rpo_current_max_lag_sequences`
- `hatrie_cache_replication_rpo_max_lag_sequences`
- `hatrie_cache_replication_rpo_required_remote_regions`
- `hatrie_cache_replication_rpo_available_remote_regions`
- `hatrie_cache_replication_rpo_missing_remote_regions`
- `hatrie_cache_replication_rto_max_millis`

The detailed `HTTPReplicator.RegionReplicationStatus()` API remains available
when callers need target or region names. These metrics do not change routing,
replication, or failover behavior; callers still own failover decisions.

See [BENCHMARK.md](BENCHMARK.md#t-u07-live-regional-rpo-prometheus-metrics)
for the measured scrape cost and the discarded full-status implementation.
