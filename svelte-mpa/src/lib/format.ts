import type { CacheEntry } from './api';

export function formatBytes(value: number): string {
  if (value < 1024) {
    return `${value} B`;
  }
  const units = ['KB', 'MB', 'GB', 'TB'];
  let scaled = value / 1024;
  let unit = 0;
  while (scaled >= 1024 && unit < units.length - 1) {
    scaled /= 1024;
    unit++;
  }
  return `${scaled.toFixed(scaled >= 10 ? 0 : 1)} ${units[unit]}`;
}

export function formatDuration(seconds: number): string {
  if (seconds < 60) {
    return `${Math.floor(seconds)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 48) {
    return `${hours}h`;
  }
  return `${Math.floor(hours / 24)}d`;
}

export function formatTTL(ttlMs: number | null): string {
  if (ttlMs === null) {
    return 'persistent';
  }
  if (ttlMs <= 0) {
    return 'expired';
  }
  return formatDuration(ttlMs / 1000);
}

export function formatRate(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

export function formatRelativeTime(value?: string): string {
  if (!value) {
    return 'never';
  }
  const elapsed = Math.max(0, (Date.now() - new Date(value).getTime()) / 1000);
  return `${formatDuration(elapsed)} ago`;
}

export function filterEntries(entries: CacheEntry[], query: string, type: string): CacheEntry[] {
  const needle = query.trim().toLowerCase();
  return entries.filter((entry) => {
    const matchesType = type === 'all' || entry.type === type;
    const matchesQuery = !needle || entry.key.toLowerCase().includes(needle);
    return matchesType && matchesQuery;
  });
}
