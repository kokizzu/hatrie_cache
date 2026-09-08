import type { ClusterTopology, ReplicationResult, TopologyNode } from './api';

export type PartitionHealthStatus = 'healthy' | 'lagging' | 'unknown' | 'maintenance';

export type PartitionHealthRow = {
  id: string;
  primary: string;
  region: string;
  replicas: string;
  lag: number | null;
  status: PartitionHealthStatus;
};

function topologyNode(nodes: TopologyNode[], id: string): TopologyNode | undefined {
  return nodes.find((node) => node.id === id);
}

export function partitionHealthRows(value: ClusterTopology | null, result: ReplicationResult | null): PartitionHealthRow[] {
  if (!value || value.version === 0) return [];
  const nodes = value.nodes ?? [];
  const lagByTarget = result?.queue?.replication_lag_by_target ?? {};
  const shardRows = value.shards ?? [];
  const rows = shardRows.length
    ? shardRows.map((shard) => ({ id: `shard-${shard.id}`, primary: shard.primary, replicas: shard.replicas ?? [] }))
    : value.mode === 'full_replica'
      ? [{
          id: 'full-replica',
          primary: value.self || nodes.find((node) => node.role === 'primary')?.id || '',
          replicas: nodes.map((node) => node.id).filter((id) => id !== value.self)
        }]
      : [];
  return rows.map(({ id, primary, replicas }) => {
    const owners = [primary, ...replicas].filter(Boolean);
    const primaryNode = topologyNode(nodes, primary);
    const ownerNodes = owners.map((owner) => topologyNode(nodes, owner));
    const knownReplicaLags = replicas.map((replica) => lagByTarget[replica]).filter((lag): lag is number => typeof lag === 'number');
    const lag = knownReplicaLags.length ? Math.max(...knownReplicaLags) : replicas.length ? null : 0;
    const maintenance = ownerNodes.some((node) => node?.maintenance);
    const status: PartitionHealthStatus = maintenance
      ? 'maintenance'
      : primaryNode && (lag === null || lag > 0)
        ? lag === null
          ? 'unknown'
          : 'lagging'
        : primaryNode
          ? 'healthy'
          : 'unknown';
    return {
      id,
      primary: primary || 'unassigned',
      region: primaryNode?.region || 'unassigned',
      replicas: replicas.length ? replicas.join(', ') : 'none',
      lag,
      status
    };
  });
}

export function partitionStatusTone(status: PartitionHealthStatus): 'green' | 'amber' | 'red' | 'blue' {
  switch (status) {
    case 'healthy':
      return 'green';
    case 'lagging':
      return 'amber';
    case 'maintenance':
      return 'red';
    default:
      return 'blue';
  }
}
