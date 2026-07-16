export type CacheHealth = {
  status: 'online' | 'degraded' | 'offline';
  node: string;
  uptime_seconds: number;
  memory_bytes: number;
  disk_spill_bytes: number;
  cleaners_running: number;
};

export type CacheStats = {
  reads: number;
  hits: number;
  misses: number;
  writes: number;
  deletes: number;
  expirations: number;
  last_hit?: string;
  last_miss?: string;
  last_write?: string;
  hit_rate: number;
  cumulative_hit_rate: number;
};

export type CacheEntry = {
  key: string;
  type:
    | 'counter'
    | 'string'
    | 'bytes'
    | 'map'
    | 'slice'
    | 'set'
    | 'priority_queue'
    | 'bloom_filter'
    | 'xor_filter'
    | 'cuckoo_filter'
    | 'roaring_bitmap'
    | 'sparse_bitset'
    | 'count_min_sketch'
    | 'hyperloglog'
    | 'top_k'
    | 'quantile_sketch'
    | 'fenwick_tree'
    | 'reservoir_sample'
    | 'radix_tree';
  ttl_ms: number | null;
  on_disk: boolean;
  size_bytes: number;
  value_preview: string;
};

export type EntriesResponse = {
  entries: CacheEntry[];
  limit?: number;
  has_more?: boolean;
  after_key?: string;
  next_after_key?: string;
};

export const DEFAULT_ENTRIES_LIMIT = 1000;

export type CommandRequest = {
  command: string;
  key: string;
  value?: string;
  subkey?: string;
  priority?: number | null;
  ttl_seconds?: number | null;
};

export type CommandResponse = {
  ok: boolean;
  message: string;
  value?: string;
};

export type StorageStatus = {
  leveldb_configured: boolean;
  store?: string;
  path?: string;
  format?: string;
  size_bytes?: number;
  error?: string;
  properties?: LevelDBProperties;
  operation: StorageOperationStatus;
  last_flush?: StorageFlushResult;
  last_compact?: StorageCompactResult;
};

export type StorageOperationStatus = {
  running: boolean;
  action?: string;
  started_at?: string;
  age_millis?: number;
};

export type LevelDBProperties = {
  stats?: string;
  sstables?: string;
  write_delay?: string;
  block_pool?: string;
};

export type StorageFlushResult = {
  store: string;
  keys: number;
  started_at: string;
  finished_at: string;
  duration_millis: number;
};

export type StorageCompactResult = {
  store: string;
  start_key?: string;
  limit_key?: string;
  size_bytes_before: number;
  size_bytes_after: number;
  size_bytes_delta: number;
  properties_before: LevelDBProperties;
  properties_after: LevelDBProperties;
  started_at: string;
  finished_at: string;
  duration_millis: number;
};

export type ReplicationQueueStats = {
  enabled: boolean;
  depth: number;
  capacity: number;
  enqueued: number;
  dropped: number;
  attempts: number;
  successes: number;
  failures: number;
  retried: number;
  oldest_queued_at?: string;
  oldest_queued_age_millis?: number;
  oldest_queued_key?: string;
  oldest_queued_targets?: string[];
  in_flight_started_at?: string;
  in_flight_age_millis?: number;
  in_flight_key?: string;
  last_retry_at?: string;
  last_retry_age_millis?: number;
  last_retry_key?: string;
  dropped_by_target?: Record<string, number>;
  failures_by_target?: Record<string, number>;
  closed: boolean;
};

export type ReplicationTargetResult = {
  node: string;
  key?: string;
  address?: string;
  ok: boolean;
  status?: number;
  error?: string;
};

export type ReplicationResult = {
  command?: string;
  key?: string;
  entries?: number;
  queued?: boolean;
  skipped: boolean;
  reason?: string;
  started_at?: string;
  finished_at?: string;
  duration_millis?: number;
  queue?: ReplicationQueueStats;
  targets?: ReplicationTargetResult[];
};

const sampleHealth: CacheHealth = {
  status: 'online',
  node: 'local-dev',
  uptime_seconds: 86423,
  memory_bytes: 384 * 1024 * 1024,
  disk_spill_bytes: 92 * 1024 * 1024,
  cleaners_running: 1
};

const sampleStats: CacheStats = {
  reads: 918240,
  hits: 866020,
  misses: 52220,
  writes: 128440,
  deletes: 9130,
  expirations: 44210,
  last_hit: new Date(Date.now() - 9000).toISOString(),
  last_miss: new Date(Date.now() - 48000).toISOString(),
  last_write: new Date(Date.now() - 12000).toISOString(),
  hit_rate: 0.9431,
  cumulative_hit_rate: 0.9431
};

const sampleEntries: CacheEntry[] = [
  {
    key: 'session:user:1024',
    type: 'map',
    ttl_ms: 3420000,
    on_disk: false,
    size_bytes: 382,
    value_preview: '{"user_id":1024,"role":"admin"}'
  },
  {
    key: 'asset:hero:raw',
    type: 'bytes',
    ttl_ms: null,
    on_disk: true,
    size_bytes: 884736,
    value_preview: '884736 bytes'
  },
  {
    key: 'counter:article:87',
    type: 'counter',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 4,
    value_preview: '4721'
  },
  {
    key: 'queue:email',
    type: 'slice',
    ttl_ms: 720000,
    on_disk: false,
    size_bytes: 1176,
    value_preview: '18 pending items'
  },
  {
    key: 'priority:jobs',
    type: 'priority_queue',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 6,
    value_preview: '6 priority items'
  },
  {
    key: 'seen:emails',
    type: 'bloom_filter',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 11984,
    value_preview: '95851 bits, 7 hashes'
  },
  {
    key: 'seen:domains:xor',
    type: 'xor_filter',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 12384,
    value_preview: '10000 items, 12384 fingerprint bytes'
  },
  {
    key: 'index:sessions',
    type: 'radix_tree',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 6144,
    value_preview: '2500 items, 381 nodes'
  },
  {
    key: 'freq:paths',
    type: 'count_min_sketch',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 32768,
    value_preview: '2048x4 counters, 918240 total'
  },
  {
    key: 'card:visitors',
    type: 'hyperloglog',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 16384,
    value_preview: '14 precision, 482120 estimated'
  },
  {
    key: 'top:paths',
    type: 'top_k',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 4096,
    value_preview: '8/100 tracked, 918240 total'
  },
  {
    key: 'latency:p95',
    type: 'quantile_sketch',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 3072,
    value_preview: '918240 samples, 318 summary points'
  },
  {
    key: 'scores:hourly',
    type: 'fenwick_tree',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 8200,
    value_preview: '1024 counters, 128440 total'
  },
  {
    key: 'sample:requests',
    type: 'reservoir_sample',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 4096,
    value_preview: '128/128 sampled, 918240 seen'
  },
  {
    key: 'ids:active64',
    type: 'sparse_bitset',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 24,
    value_preview: '12 integers, 3 containers'
  },
  {
    key: 'tags:active',
    type: 'set',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 4,
    value_preview: '4 members'
  },
  {
    key: 'profile:name:1024',
    type: 'string',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 12,
    value_preview: 'Ivi Cache'
  }
];

const sampleStorageStatus: StorageStatus = {
  leveldb_configured: false,
  operation: {
    running: false
  }
};

const sampleReplicationResult: ReplicationResult = {
  skipped: true,
  reason: 'replication is not configured'
};

async function readJSONWithFallback<T>(path: string, fallback: T, init?: RequestInit): Promise<T> {
  try {
    const response = await fetch(path, {
      headers: { accept: 'application/json', ...(init?.headers ?? {}) },
      ...init
    });
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`);
    }
    return (await response.json()) as T;
  } catch {
    return fallback;
  }
}

async function postJSON<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    method: 'POST',
    headers: { accept: 'application/json', 'content-type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    throw new Error(await responseErrorMessage(response));
  }
  return (await response.json()) as T;
}

async function responseErrorMessage(response: Response): Promise<string> {
  try {
    const payload = (await response.json()) as Partial<CommandResponse>;
    if (payload.message) {
      return payload.message;
    }
  } catch {
    // Fall back to HTTP status text below.
  }
  return `${response.status} ${response.statusText}`.trim();
}

export function sampleCommandResponse(request: CommandRequest): CommandResponse {
  const key = request.key.trim();
  if (!key) {
    return { ok: false, message: 'A key is required.' };
  }
  switch (request.command) {
    case 'GET':
      return { ok: true, message: `Read ${key}.`, value: sampleEntries.find((entry) => entry.key === key)?.value_preview ?? '' };
    case 'DEL':
      return { ok: true, message: `Deleted ${key}.` };
    case 'EXPIRE':
      return { ok: true, message: `Set TTL for ${key} to ${request.ttl_seconds ?? 0}s.` };
    default:
      return { ok: true, message: `Stored ${key}.` };
  }
}

export async function loadHealth(): Promise<CacheHealth> {
  return readJSONWithFallback<CacheHealth>('/api/health', sampleHealth);
}

export async function loadStats(): Promise<CacheStats> {
  return readJSONWithFallback<CacheStats>('/api/stats', sampleStats);
}

export async function loadEntries(prefix = '', limit = 0, afterKey = ''): Promise<EntriesResponse> {
  const query = new URLSearchParams();
  if (prefix) query.set('prefix', prefix);
  if (limit > 0) query.set('limit', String(limit));
  if (afterKey) query.set('after_key', afterKey);
  const encoded = query.toString();
  const path = encoded ? `/api/entries?${encoded}` : '/api/entries';
  const matchedEntries = sampleEntries
    .filter((entry) => entry.key.startsWith(prefix))
    .sort((left, right) => left.key.localeCompare(right.key));
  const cursorEntries = afterKey ? matchedEntries.filter((entry) => entry.key > afterKey) : matchedEntries;
  const entries = cursorEntries.slice(0, limit > 0 ? limit : undefined);
  const hasMore = limit > 0 && cursorEntries.length > limit;
  const fallback = {
    entries,
    ...(limit > 0 ? { limit, has_more: hasMore } : {}),
    ...(afterKey ? { after_key: afterKey } : {}),
    ...(hasMore && entries.length > 0 ? { next_after_key: entries[entries.length - 1].key } : {})
  };
  return readJSONWithFallback<EntriesResponse>(path, fallback);
}

export async function runCommand(request: CommandRequest): Promise<CommandResponse> {
  return postJSON<CommandResponse>('/api/commands', request);
}

export async function loadStorageStatus(): Promise<StorageStatus> {
  return readJSONWithFallback<StorageStatus>('/api/storage', sampleStorageStatus);
}

export async function flushStorage(): Promise<StorageFlushResult> {
  return postJSON<StorageFlushResult>('/api/storage/flush', {});
}

export async function compactStorage(startKey = '', limitKey = ''): Promise<StorageCompactResult> {
  const request: { start_key?: string; limit_key?: string } = {};
  const start = startKey.trim();
  const limit = limitKey.trim();
  if (start) request.start_key = start;
  if (limit) request.limit_key = limit;
  return postJSON<StorageCompactResult>('/api/storage/compact', request);
}

export async function loadReplicationStatus(): Promise<ReplicationResult> {
  return readJSONWithFallback<ReplicationResult>('/api/replication', sampleReplicationResult);
}

export async function syncReplication(prefix = ''): Promise<ReplicationResult> {
  const trimmedPrefix = prefix.trim();
  return postJSON<ReplicationResult>('/api/replication', trimmedPrefix ? { prefix: trimmedPrefix } : {});
}
