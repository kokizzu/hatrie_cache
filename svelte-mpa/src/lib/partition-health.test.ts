import { describe, expect, it } from 'vitest';
import { partitionHealthRows, partitionStatusTone } from './partition-health';

describe('partition health', () => {
  it('derives the worst known replica lag per shard', () => {
    const rows = partitionHealthRows(
      {
        version: 1,
        mode: 'sharded',
        nodes: [
          { id: 'node-a', address: '', region: 'asia' },
          { id: 'node-b', address: '', region: 'europe' },
          { id: 'node-c', address: '', region: 'us' }
        ],
        shards: [{ id: 3, primary: 'node-a', replicas: ['node-b', 'node-c'] }]
      },
      { skipped: false, queue: { enabled: true, depth: 0, capacity: 4, replication_lag_by_target: { 'node-b': 0, 'node-c': 4 }, enqueued: 0, dropped: 0, attempts: 0, successes: 0, failures: 0, retried: 0, closed: false } }
    );

    expect(rows).toEqual([
      {
        id: 'shard-3',
        primary: 'node-a',
        region: 'asia',
        replicas: 'node-b, node-c',
        lag: 4,
        status: 'lagging'
      }
    ]);
  });

  it('reports unknown lag, maintenance, and full-replica fallback explicitly', () => {
    const unknown = partitionHealthRows(
      {
        version: 1,
        mode: 'sharded',
        nodes: [{ id: 'node-a', address: '', region: 'asia' }, { id: 'node-b', address: '', region: 'europe' }],
        shards: [{ id: 1, primary: 'node-a', replicas: ['node-b'] }]
      },
      { skipped: false, queue: { enabled: true, depth: 0, capacity: 4, enqueued: 0, dropped: 0, attempts: 0, successes: 0, failures: 0, retried: 0, closed: false } }
    );
    expect(unknown[0].status).toBe('unknown');
    expect(unknown[0].lag).toBeNull();

    const maintenance = partitionHealthRows(
      {
        version: 1,
        mode: 'sharded',
        nodes: [{ id: 'node-a', address: '', maintenance: true }],
        shards: [{ id: 1, primary: 'node-a' }]
      },
      null
    );
    expect(maintenance[0].status).toBe('maintenance');

    const fullReplica = partitionHealthRows(
      {
        version: 1,
        mode: 'full_replica',
        self: 'node-a',
        nodes: [{ id: 'node-a', address: '', role: 'primary' }, { id: 'node-b', address: '' }]
      },
      null
    );
    expect(fullReplica[0]).toMatchObject({ id: 'full-replica', primary: 'node-a', replicas: 'node-b', status: 'unknown', lag: null });
  });

  it('maps health states to the existing admin palette', () => {
    expect(partitionStatusTone('healthy')).toBe('green');
    expect(partitionStatusTone('lagging')).toBe('amber');
    expect(partitionStatusTone('maintenance')).toBe('red');
    expect(partitionStatusTone('unknown')).toBe('blue');
  });
});
