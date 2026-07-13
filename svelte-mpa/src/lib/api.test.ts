import { describe, expect, it } from 'vitest';
import { sampleCommandResponse } from './api';

describe('command fallback', () => {
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
});
