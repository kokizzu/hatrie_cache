# Partition Health

The Admin page at `/admin.html` includes a read-only **Partition Health**
table. It is an operator view for the existing explicit partition topology; it
does not change routing, fail over a node, or enable a monitoring server.

## Data Sources

The page loads both endpoints on every refresh:

- `GET /api/topology` supplies partition ownership and node metadata.
- `GET /api/replication` supplies per-target replication lag.

Rows use the topology's shard ownership when it is available. A topology with
no shard list can still show a `full-replica` row for the current replication
set. A topology version of `0`, or a response without ownership data, is shown
as unavailable instead of being interpreted as healthy.

## Columns And Status

- **Partition**: the topology shard ID, or `full-replica` for the fallback row.
- **Primary**: the node ID that owns writes for the partition.
- **Region**: the primary node's declared region, when present.
- **Replicas**: the number of secondary nodes listed for the partition.
- **Max lag**: the greatest reported `replication_lag_by_target` value among
  the partition's replicas, in sequence units.
- **Status**: `healthy`, `lagging`, `maintenance`, or `unknown`.

The largest known positive lag marks a partition as `lagging`. A missing lag
report is displayed as `unreported` and produces `unknown`; it is never treated
as zero. A node in maintenance produces `maintenance`, and missing primary
ownership remains `unknown`. A known primary with no replicas has zero known
lag and is displayed as `healthy`.

This dashboard is intentionally observational. Operators should use the
existing topology, replication, backup, and failover commands for changes.
