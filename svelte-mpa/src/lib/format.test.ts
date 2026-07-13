import { describe, expect, it } from 'vitest';
import { filterEntries, formatBytes, formatRate, formatTTL } from './format';
import type { CacheEntry } from './api';

const entries: CacheEntry[] = [
  { key: 'user:1', type: 'map', ttl_ms: null, on_disk: false, size_bytes: 20, value_preview: '{}' },
  { key: 'asset:large', type: 'bytes', ttl_ms: 5000, on_disk: true, size_bytes: 70000, value_preview: 'bytes' },
  { key: 'freq:paths', type: 'count_min_sketch', ttl_ms: null, on_disk: false, size_bytes: 32768, value_preview: '2048x4 counters' },
  { key: 'card:visitors', type: 'hyperloglog', ttl_ms: null, on_disk: false, size_bytes: 16384, value_preview: '14 precision' }
];

describe('format helpers', () => {
  it('formats bytes with stable units', () => {
    expect(formatBytes(512)).toBe('512 B');
    expect(formatBytes(1536)).toBe('1.5 KB');
    expect(formatBytes(5 * 1024 * 1024)).toBe('5.0 MB');
  });

  it('formats TTL and rates', () => {
    expect(formatTTL(null)).toBe('persistent');
    expect(formatTTL(0)).toBe('expired');
    expect(formatTTL(65000)).toBe('1m');
    expect(formatRate(0.875)).toBe('87.5%');
  });

  it('filters entries by text and type', () => {
    expect(filterEntries(entries, 'USER', 'all')).toHaveLength(1);
    expect(filterEntries(entries, '', 'bytes')).toEqual([entries[1]]);
    expect(filterEntries(entries, '', 'count_min_sketch')).toEqual([entries[2]]);
    expect(filterEntries(entries, '', 'hyperloglog')).toEqual([entries[3]]);
    expect(filterEntries(entries, 'missing', 'all')).toHaveLength(0);
  });
});
