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
  type: 'counter' | 'string' | 'bytes' | 'map' | 'slice';
  ttl_ms: number | null;
  on_disk: boolean;
  size_bytes: number;
  value_preview: string;
};

export type EntriesResponse = {
  entries: CacheEntry[];
};

export type CommandRequest = {
  command: string;
  key: string;
  value?: string;
  ttl_seconds?: number | null;
};

export type CommandResponse = {
  ok: boolean;
  message: string;
  value?: string;
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
    key: 'profile:name:1024',
    type: 'string',
    ttl_ms: null,
    on_disk: false,
    size_bytes: 12,
    value_preview: 'Ivi Cache'
  }
];

async function readJSON<T>(path: string, fallback: T, init?: RequestInit): Promise<T> {
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
  return readJSON<CacheHealth>('/api/health', sampleHealth);
}

export async function loadStats(): Promise<CacheStats> {
  return readJSON<CacheStats>('/api/stats', sampleStats);
}

export async function loadEntries(prefix = ''): Promise<EntriesResponse> {
  const path = prefix ? `/api/entries?prefix=${encodeURIComponent(prefix)}` : '/api/entries';
  const fallback = {
    entries: sampleEntries.filter((entry) => entry.key.startsWith(prefix))
  };
  return readJSON<EntriesResponse>(path, fallback);
}

export async function runCommand(request: CommandRequest): Promise<CommandResponse> {
  return readJSON<CommandResponse>('/api/commands', sampleCommandResponse(request), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request)
  });
}
