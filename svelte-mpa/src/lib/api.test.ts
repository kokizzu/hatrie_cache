import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  compactStorage,
  DEFAULT_ENTRIES_LIMIT,
  flushStorage,
  loadEntries,
  loadReplicationStatus,
  loadStorageStatus,
  runCommand,
  sampleCommandResponse,
  syncReplication
} from './api';

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('command fallback', () => {
  it('uses the shared bounded entries limit in requests', async () => {
    const fetchMock = vi.fn(async (path: string | URL | Request) => {
      expect(path).toBe(`/api/entries?limit=${DEFAULT_ENTRIES_LIMIT}`);
      return new Response(
        JSON.stringify({
          entries: [],
          limit: DEFAULT_ENTRIES_LIMIT,
          has_more: false
        }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' }
        }
      );
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(loadEntries('', DEFAULT_ENTRIES_LIMIT)).resolves.toEqual({
      entries: [],
      limit: DEFAULT_ENTRIES_LIMIT,
      has_more: false
    });
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it('loads entries with prefix, limit, and cursor query params', async () => {
    const fetchMock = vi.fn(async (path: string | URL | Request) => {
      expect(path).toBe('/api/entries?prefix=session%3A&limit=2&after_key=session%3A2');
      return new Response(
        JSON.stringify({
          entries: [],
          limit: 2,
          has_more: true,
          after_key: 'session:2',
          next_after_key: 'session:4'
        }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' }
        }
      );
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(loadEntries('session:', 2, 'session:2')).resolves.toEqual({
      entries: [],
      limit: 2,
      has_more: true,
      after_key: 'session:2',
      next_after_key: 'session:4'
    });
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it('falls back to cursor-paged sample entries', async () => {
    const fetchMock = vi.fn(async () => {
      throw new Error('offline');
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(loadEntries('', 2, 'asset:hero:raw')).resolves.toMatchObject({
      entries: [
        expect.objectContaining({ key: 'card:visitors' }),
        expect.objectContaining({ key: 'counter:article:87' })
      ],
      limit: 2,
      has_more: true,
      after_key: 'asset:hero:raw',
      next_after_key: 'counter:article:87'
    });
  });

  it('rejects commands without a key', () => {
    expect(sampleCommandResponse({ command: 'GET', key: ' ' })).toEqual({
      ok: false,
      message: 'A key is required.'
    });
  });

  it('returns deterministic command messages', () => {
    expect(sampleCommandResponse({ command: 'DEL', key: 'session:1' })).toEqual({
      ok: true,
      message: 'Deleted session:1.'
    });
    expect(sampleCommandResponse({ command: 'EXPIRE', key: 'session:1', ttl_seconds: 30 })).toEqual({
      ok: true,
      message: 'Set TTL for session:1 to 30s.'
    });
  });

  it('posts Fenwick command payloads', async () => {
    const fetchMock = vi.fn(async (_path: string | URL | Request, init?: RequestInit) => {
      expect(_path).toBe('/api/commands');
      expect(init?.method).toBe('POST');
      expect(init?.headers).toMatchObject({ 'content-type': 'application/json' });
      expect(JSON.parse(String(init?.body))).toEqual({
        command: 'ADDFW',
        key: 'scores:hourly',
        value: '13',
        subkey: '7'
      });
      return new Response(
        JSON.stringify({
          ok: true,
          message: 'updated fenwick tree',
          value: '{"index":13,"delta":7,"value":7}'
        }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' }
        }
      );
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(
      runCommand({
        command: 'ADDFW',
        key: 'scores:hourly',
        value: '13',
        subkey: '7'
      })
    ).resolves.toEqual({
      ok: true,
      message: 'updated fenwick tree',
      value: '{"index":13,"delta":7,"value":7}'
    });
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it('posts reservoir sample command payloads', async () => {
    const fetchMock = vi.fn(async (_path: string | URL | Request, init?: RequestInit) => {
      expect(_path).toBe('/api/commands');
      expect(init?.method).toBe('POST');
      expect(init?.headers).toMatchObject({ 'content-type': 'application/json' });
      expect(JSON.parse(String(init?.body))).toEqual({
        command: 'ADDRS',
        key: 'sample:requests',
        value: '/api/users'
      });
      return new Response(
        JSON.stringify({
          ok: true,
          message: 'added reservoir sample values',
          value: '{"accepted":true,"seen":1,"tracked":1,"capacity":128}'
        }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' }
        }
      );
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(
      runCommand({
        command: 'ADDRS',
        key: 'sample:requests',
        value: '/api/users'
      })
    ).resolves.toEqual({
      ok: true,
      message: 'added reservoir sample values',
      value: '{"accepted":true,"seen":1,"tracked":1,"capacity":128}'
    });
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it('loads storage and replication admin status', async () => {
    const fetchMock = vi.fn(async (path: string | URL | Request) => {
      if (path === '/api/storage') {
        return new Response(JSON.stringify({ leveldb_configured: true }), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }
      expect(path).toBe('/api/replication');
      return new Response(JSON.stringify({ skipped: false, queue: { enabled: true, depth: 1, capacity: 4, enqueued: 2, dropped: 0, attempts: 2, successes: 1, failures: 1, retried: 1, closed: false } }), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      });
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(loadStorageStatus()).resolves.toEqual({ leveldb_configured: true });
    await expect(loadReplicationStatus()).resolves.toMatchObject({
      skipped: false,
      queue: { depth: 1, capacity: 4 }
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('posts storage admin operations', async () => {
    const fetchMock = vi.fn(async (path: string | URL | Request, init?: RequestInit) => {
      expect(init?.method).toBe('POST');
      expect(init?.headers).toMatchObject({ 'content-type': 'application/json' });
      if (path === '/api/storage/flush') {
        expect(JSON.parse(String(init?.body))).toEqual({});
        return new Response(JSON.stringify({ store: 'leveldb', keys: 3, started_at: '2026-01-01T00:00:00Z', finished_at: '2026-01-01T00:00:01Z', duration_millis: 1000 }), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }
      expect(path).toBe('/api/storage/compact');
      expect(JSON.parse(String(init?.body))).toEqual({ start_key: 'alpha', limit_key: 'omega' });
      return new Response(
        JSON.stringify({
          store: 'leveldb',
          start_key: 'alpha',
          limit_key: 'omega',
          size_bytes_before: 4096,
          size_bytes_after: 2048,
          size_bytes_delta: -2048,
          properties_before: { stats: 'before' },
          properties_after: { stats: 'after' },
          started_at: '2026-01-01T00:00:00Z',
          finished_at: '2026-01-01T00:00:01Z',
          duration_millis: 1000
        }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' }
        }
      );
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(flushStorage()).resolves.toMatchObject({ store: 'leveldb', keys: 3 });
    await expect(compactStorage(' alpha ', ' omega ')).resolves.toMatchObject({
      start_key: 'alpha',
      limit_key: 'omega',
      size_bytes_delta: -2048
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('posts replication prefix sync operations', async () => {
    const fetchMock = vi.fn(async (path: string | URL | Request, init?: RequestInit) => {
      expect(path).toBe('/api/replication');
      expect(init?.method).toBe('POST');
      expect(init?.headers).toMatchObject({ 'content-type': 'application/json' });
      expect(JSON.parse(String(init?.body))).toEqual({ prefix: 'session:' });
      return new Response(JSON.stringify({ command: 'SYNC', key: 'session:', entries: 8, skipped: false, duration_millis: 12, targets: [] }), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      });
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(syncReplication(' session: ')).resolves.toMatchObject({
      command: 'SYNC',
      key: 'session:',
      entries: 8,
      skipped: false
    });
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it('rejects failed admin mutations instead of returning sample success', async () => {
    const fetchMock = vi.fn(async () => {
      return new Response(JSON.stringify({ ok: false, message: 'leveldb store is not configured' }), {
        status: 409,
        statusText: 'Conflict',
        headers: { 'content-type': 'application/json' }
      });
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(flushStorage()).rejects.toThrow('leveldb store is not configured');
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it('rejects failed commands instead of using command fallbacks', async () => {
    const fetchMock = vi.fn(async () => {
      return new Response(JSON.stringify({ ok: false, message: 'writes are disabled' }), {
        status: 403,
        statusText: 'Forbidden',
        headers: { 'content-type': 'application/json' }
      });
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(runCommand({ command: 'SETSTR', key: 'session:1', value: 'ivi' })).rejects.toThrow('writes are disabled');
    expect(fetchMock).toHaveBeenCalledOnce();
  });
});
