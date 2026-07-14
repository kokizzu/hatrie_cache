import { afterEach, describe, expect, it, vi } from 'vitest';
import { DEFAULT_ENTRIES_LIMIT, loadEntries, runCommand, sampleCommandResponse } from './api';

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

  it('loads entries with prefix and limit query params', async () => {
    const fetchMock = vi.fn(async (path: string | URL | Request) => {
      expect(path).toBe('/api/entries?prefix=session%3A&limit=2');
      return new Response(
        JSON.stringify({
          entries: [],
          limit: 2,
          has_more: true
        }),
        {
          status: 200,
          headers: { 'content-type': 'application/json' }
        }
      );
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(loadEntries('session:', 2)).resolves.toEqual({
      entries: [],
      limit: 2,
      has_more: true
    });
    expect(fetchMock).toHaveBeenCalledOnce();
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
});
