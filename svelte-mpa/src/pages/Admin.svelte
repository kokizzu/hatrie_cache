<script lang="ts">
  import { onMount } from 'svelte';
  import { Activity, Clock3, Database, HardDrive, RefreshCw, Send, ShieldCheck } from '@lucide/svelte';
  import Shell from '../components/Shell.svelte';
  import StatTile from '../components/StatTile.svelte';
  import {
    compactStorage,
    flushStorage,
    loadAuditEvents,
    loadReplicationStatus,
    loadStorageStatus,
    syncReplication,
    type AuditEvent,
    type AuditStatus,
    type ReplicationDeadLetter,
    type ReplicationResult,
    type ReplicationQueueStats,
    type StorageCompactResult,
    type StorageFlushResult,
    type StorageStatus
  } from '../lib/api';
  import { formatBytes, formatRelativeTime } from '../lib/format';

  let storage: StorageStatus | null = null;
  let replication: ReplicationResult | null = null;
  let audit: AuditStatus | null = null;
  let lastFlush: StorageFlushResult | null = null;
  let lastCompact: StorageCompactResult | null = null;
  let syncResult: ReplicationResult | null = null;
  let compactStartKey = '';
  let compactLimitKey = '';
  let syncPrefix = '';
  let storageMessage = '';
  let replicationMessage = '';
  let loading = true;
  let storageAction: '' | 'flush' | 'compact' = '';
  let replicationAction: '' | 'sync' = '';
  let confirmFlush = false;
  let confirmCompact = false;
  let confirmSync = false;

  async function refresh() {
    loading = true;
    try {
      const [nextStorage, nextReplication, nextAudit] = await Promise.all([loadStorageStatus(), loadReplicationStatus(), loadAuditEvents()]);
      storage = nextStorage;
      replication = nextReplication;
      audit = nextAudit;
    } finally {
      loading = false;
    }
  }

  async function runFlush() {
    if (!confirmFlush) {
      storageMessage = 'Confirm flush before running it.';
      return;
    }
    storageAction = 'flush';
    storageMessage = '';
    try {
      lastFlush = await flushStorage();
      storageMessage = `Flushed ${lastFlush.keys.toLocaleString()} keys in ${formatMillis(lastFlush.duration_millis)}.`;
      await refresh();
    } catch (error) {
      storageMessage = error instanceof Error ? error.message : 'Storage flush failed.';
    } finally {
      confirmFlush = false;
      storageAction = '';
    }
  }

  async function runCompact() {
    if (!confirmCompact) {
      storageMessage = 'Confirm compaction before running it.';
      return;
    }
    storageAction = 'compact';
    storageMessage = '';
    try {
      lastCompact = await compactStorage(compactStartKey, compactLimitKey);
      storageMessage = `Compacted ${formatBytes(lastCompact.size_bytes_before)} to ${formatBytes(lastCompact.size_bytes_after)} in ${formatMillis(lastCompact.duration_millis)}.`;
      await refresh();
    } catch (error) {
      storageMessage = error instanceof Error ? error.message : 'Storage compaction failed.';
    } finally {
      confirmCompact = false;
      storageAction = '';
    }
  }

  async function runSync() {
    if (!confirmSync) {
      replicationMessage = 'Confirm sync before running it.';
      return;
    }
    replicationAction = 'sync';
    replicationMessage = '';
    try {
      syncResult = await syncReplication(syncPrefix);
      replicationMessage = syncResult.skipped
        ? (syncResult.reason ?? 'Sync skipped.')
        : `Synced ${(syncResult.entries ?? 0).toLocaleString()} entries in ${formatMillis(syncResult.duration_millis)}.`;
      await refresh();
    } catch (error) {
      replicationMessage = error instanceof Error ? error.message : 'Replication sync failed.';
    } finally {
      confirmSync = false;
      replicationAction = '';
    }
  }

  function formatMillis(value?: number): string {
    const millis = Math.max(0, value ?? 0);
    if (millis < 1000) return `${millis} ms`;
    return `${(millis / 1000).toFixed(millis < 10000 ? 2 : 1)} s`;
  }

  function queueFill(queue?: ReplicationQueueStats): string {
    if (!queue || queue.capacity <= 0) return '0%';
    return `${Math.min(100, Math.round((queue.depth / queue.capacity) * 100))}%`;
  }

  function replicationTone(health?: string): 'blue' | 'green' | 'amber' | 'red' {
    switch (health) {
      case 'ok':
        return 'green';
      case 'degraded':
        return 'amber';
      case 'unhealthy':
        return 'red';
      default:
        return 'blue';
    }
  }

  function replicationHealthText(result: ReplicationResult | null): string {
    if (!result || result.health_score === undefined) return 'unknown';
    return `${result.health_score}/100 ${result.health ?? ''}`.trim();
  }

  function formatSignedBytes(value: number): string {
    if (value === 0) return '0 B';
    const sign = value > 0 ? '+' : '-';
    return `${sign}${formatBytes(Math.abs(value))}`;
  }

  function targetRows(values?: Record<string, number>): [string, number][] {
    return Object.entries(values ?? {}).sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
  }

  function compactPropertyText(result: StorageCompactResult | null): string {
    const statusProperties = storage?.properties;
    if (!result) {
      return statusProperties?.stats || statusProperties?.sstables || statusProperties?.write_delay || statusProperties?.block_pool || 'No LevelDB properties reported.';
    }
    const props = result.properties_after;
    return props.stats || props.sstables || props.write_delay || props.block_pool || 'No LevelDB properties reported.';
  }

  function auditResult(event: AuditEvent): string {
    if (event.ok) {
      return event.status ? `ok ${event.status}` : 'ok';
    }
    return event.status ? String(event.status) : 'failed';
  }

  function auditSubject(event: AuditEvent): string {
    return event.key || event.command || event.path || event.action;
  }

  function deadLetterTargets(deadLetter: ReplicationDeadLetter): string {
    return (deadLetter.targets ?? [])
      .map((target) => target.node || target.address || target.error || 'target')
      .join(', ');
  }

  onMount(refresh);

  $: queue = replication?.queue;
  $: operation = storage?.operation;
  $: effectiveLastFlush = lastFlush ?? storage?.last_flush ?? null;
  $: effectiveLastCompact = lastCompact ?? storage?.last_compact ?? null;
  $: targets = replication?.targets ?? [];
  $: auditEvents = audit?.events ?? [];
  $: deadLetters = replication?.dead_letters ?? [];
  $: dropsByTarget = targetRows(queue?.dropped_by_target);
  $: failuresByTarget = targetRows(queue?.failures_by_target);
</script>

<Shell active="admin">
  <header class="page-header">
    <div>
      <p>Operations</p>
      <h1>Admin</h1>
    </div>
    <button class="icon-button" type="button" on:click={refresh} aria-label="Refresh admin status" title="Refresh admin status">
      <RefreshCw size={18} class={loading ? 'spin' : ''} />
    </button>
  </header>

  <section class="stats-grid">
    <StatTile label="LevelDB" value={storage?.leveldb_configured ? 'enabled' : 'off'} detail={storage?.format ? `${storage.format} format` : 'storage engine'} tone="blue" icon={Database} />
    <StatTile label="Storage size" value={formatBytes(storage?.size_bytes ?? 0)} detail={operation?.running ? `${operation.action} running` : 'on disk'} tone={operation?.running ? 'amber' : 'green'} icon={HardDrive} />
    <StatTile label="Replication" value={replicationHealthText(replication)} detail={replication?.health_reason ?? 'health score'} tone={replicationTone(replication?.health)} icon={Activity} />
    <StatTile label="Oldest" value={queue?.oldest_queued_age_millis ? formatMillis(queue.oldest_queued_age_millis) : 'none'} detail={queue?.oldest_queued_key ?? 'queued key'} tone="amber" icon={Clock3} />
  </section>

  <section class="admin-layout">
    <div class="panel">
      <div class="panel-heading">
        <div>
          <h2>LevelDB Storage</h2>
          <p>{storage?.path ?? (storage?.leveldb_configured ? 'Configured' : 'Not configured')}</p>
        </div>
        <Database size={18} aria-hidden="true" />
      </div>

      {#if storageMessage}
        <p class="notice">{storageMessage}</p>
      {/if}
      {#if storage?.error}
        <p class="notice danger-notice">{storage.error}</p>
      {/if}

      <div class="action-grid">
        <div class="danger-action">
          <label class="checkbox-row confirm-row">
            <input type="checkbox" bind:checked={confirmFlush} disabled={!storage?.leveldb_configured || Boolean(storageAction)} />
            <span>Confirm flush</span>
          </label>
          <button class="primary-button" type="button" on:click={runFlush} disabled={!storage?.leveldb_configured || Boolean(storageAction) || !confirmFlush}>
            <HardDrive size={17} aria-hidden="true" />
            Flush
          </button>
        </div>

        <div class="compact-form">
          <label>
            <span>Start key</span>
            <input bind:value={compactStartKey} placeholder="alpha" disabled={!storage?.leveldb_configured || Boolean(storageAction)} />
          </label>
          <label>
            <span>Limit key</span>
            <input bind:value={compactLimitKey} placeholder="omega" disabled={!storage?.leveldb_configured || Boolean(storageAction)} />
          </label>
          <label class="checkbox-row confirm-row">
            <input type="checkbox" bind:checked={confirmCompact} disabled={!storage?.leveldb_configured || Boolean(storageAction)} />
            <span>Confirm compact</span>
          </label>
          <button class="secondary-button" type="button" on:click={runCompact} disabled={!storage?.leveldb_configured || Boolean(storageAction) || !confirmCompact}>
            <RefreshCw size={17} class={storageAction === 'compact' ? 'spin' : ''} aria-hidden="true" />
            Compact
          </button>
        </div>
      </div>

      <dl class="detail-list">
        <div><dt>Format</dt><dd>{storage?.format ?? 'none'}</dd></div>
        <div><dt>Size</dt><dd>{formatBytes(storage?.size_bytes ?? 0)}</dd></div>
        <div><dt>Operation</dt><dd>{operation?.running ? `${operation.action} for ${formatMillis(operation.age_millis)}` : 'idle'}</dd></div>
        <div><dt>Last flush</dt><dd>{effectiveLastFlush ? formatRelativeTime(effectiveLastFlush.finished_at) : 'never'}</dd></div>
        <div><dt>Flush duration</dt><dd>{effectiveLastFlush ? formatMillis(effectiveLastFlush.duration_millis) : '0 ms'}</dd></div>
        <div><dt>Compaction delta</dt><dd>{effectiveLastCompact ? formatSignedBytes(effectiveLastCompact.size_bytes_delta) : '0 B'}</dd></div>
        <div><dt>Compaction range</dt><dd>{effectiveLastCompact ? `${effectiveLastCompact.start_key || '*'} to ${effectiveLastCompact.limit_key || '*'}` : '*'}</dd></div>
      </dl>

      <pre class="property-box">{compactPropertyText(effectiveLastCompact)}</pre>
    </div>

    <div class="panel">
      <div class="panel-heading">
        <div>
          <h2>Replication</h2>
          <p>{replication?.skipped ? replication.reason : replication?.command ?? 'Last result'}</p>
        </div>
        <Activity size={18} aria-hidden="true" />
      </div>

      {#if replicationMessage}
        <p class="notice">{replicationMessage}</p>
      {/if}

      <div class="sync-form">
        <label>
          <span>Prefix</span>
          <input bind:value={syncPrefix} placeholder="session:" disabled={replicationAction === 'sync'} />
        </label>
        <label class="checkbox-row confirm-row">
          <input type="checkbox" bind:checked={confirmSync} disabled={replicationAction === 'sync'} />
          <span>Confirm sync</span>
        </label>
        <button class="primary-button" type="button" on:click={runSync} disabled={replicationAction === 'sync' || !confirmSync}>
          <Send size={17} aria-hidden="true" />
          Sync
        </button>
      </div>

      <dl class="detail-list">
        <div><dt>Health</dt><dd>{replicationHealthText(replication)}</dd></div>
        <div><dt>Health reason</dt><dd>{replication?.health_reason ?? 'unknown'}</dd></div>
        <div><dt>Dead letters</dt><dd>{(replication?.dead_letter_count ?? 0).toLocaleString()}</dd></div>
        <div><dt>Queue fill</dt><dd>{queueFill(queue)}</dd></div>
        <div><dt>Enqueued</dt><dd>{(queue?.enqueued ?? 0).toLocaleString()}</dd></div>
        <div><dt>Attempts</dt><dd>{(queue?.attempts ?? 0).toLocaleString()}</dd></div>
        <div><dt>Retries</dt><dd>{(queue?.retried ?? 0).toLocaleString()}</dd></div>
        <div><dt>In flight</dt><dd>{queue?.in_flight_key ?? 'none'}</dd></div>
        <div><dt>Last retry</dt><dd>{queue?.last_retry_at ? formatRelativeTime(queue.last_retry_at) : 'never'}</dd></div>
      </dl>

      <div class="meter" aria-label="Replication queue fill">
        <span style={`width: ${queueFill(queue)}`}></span>
      </div>
    </div>
  </section>

  <section class="panel">
    <div class="panel-heading">
      <div>
        <h2>Replication Targets</h2>
        <p>{targets.length ? `${targets.length.toLocaleString()} target results` : 'No target results'}</p>
      </div>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr><th>Node</th><th>Address</th><th>Key</th><th>Status</th><th>Error</th></tr>
        </thead>
        <tbody>
          {#each targets as target}
            <tr>
              <td>{target.node}</td>
              <td>{target.address ?? ''}</td>
              <td><code>{target.key ?? ''}</code></td>
              <td>{target.ok ? 'ok' : target.status || 'failed'}</td>
              <td>{target.error ?? ''}</td>
            </tr>
          {/each}
          {#if !targets.length}
            <tr><td colspan="5">No target results</td></tr>
          {/if}
        </tbody>
      </table>
    </div>
  </section>

  <section class="panel">
    <div class="panel-heading">
      <div>
        <h2>Audit Trail</h2>
        <p>{audit?.configured ? `${auditEvents.length.toLocaleString()} recent events` : 'Not configured'}</p>
      </div>
      <ShieldCheck size={18} aria-hidden="true" />
    </div>

    {#if audit && !audit.configured}
      <p class="notice">Set AUDIT_LOG_PATH to persist and view recent dangerous admin actions.</p>
    {/if}

    <div class="table-wrap">
      <table>
        <thead>
          <tr><th>Time</th><th>Action</th><th>Result</th><th>Method</th><th>Subject</th><th>Message</th></tr>
        </thead>
        <tbody>
          {#each auditEvents as event}
            <tr>
              <td>{formatRelativeTime(event.time)}</td>
              <td>{event.action}</td>
              <td><span class:ok={event.ok} class="result-pill">{auditResult(event)}</span></td>
              <td>{event.method ?? event.protocol ?? ''}</td>
              <td><code>{auditSubject(event)}</code></td>
              <td>{event.message ?? ''}</td>
            </tr>
          {/each}
          {#if !auditEvents.length}
            <tr><td colspan="6">No audit events</td></tr>
          {/if}
        </tbody>
      </table>
    </div>
  </section>

  {#if deadLetters.length}
    <section class="panel">
      <div class="panel-heading">
        <div>
          <h2>Replication Dead Letters</h2>
          <p>{deadLetters.length.toLocaleString()} retained failures</p>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>ID</th><th>Failed</th><th>Command</th><th>Key</th><th>Attempts</th><th>Targets</th><th>Reason</th></tr>
          </thead>
          <tbody>
            {#each deadLetters as deadLetter}
              <tr>
                <td>{deadLetter.id}</td>
                <td>{deadLetter.failed_at ? formatRelativeTime(deadLetter.failed_at) : ''}</td>
                <td>{deadLetter.command ?? ''}</td>
                <td><code>{deadLetter.key ?? ''}</code></td>
                <td>{deadLetter.attempts}</td>
                <td>{deadLetterTargets(deadLetter)}</td>
                <td>{deadLetter.reason ?? ''}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </section>
  {/if}

  {#if dropsByTarget.length || failuresByTarget.length}
    <section class="admin-layout">
      <div class="panel">
        <div class="panel-heading">
          <div>
            <h2>Drops By Target</h2>
            <p>{dropsByTarget.length.toLocaleString()} targets</p>
          </div>
        </div>
        <dl class="detail-list">
          {#each dropsByTarget as [target, count]}
            <div><dt>{target}</dt><dd>{count.toLocaleString()}</dd></div>
          {/each}
        </dl>
      </div>

      <div class="panel">
        <div class="panel-heading">
          <div>
            <h2>Failures By Target</h2>
            <p>{failuresByTarget.length.toLocaleString()} targets</p>
          </div>
        </div>
        <dl class="detail-list">
          {#each failuresByTarget as [target, count]}
            <div><dt>{target}</dt><dd>{count.toLocaleString()}</dd></div>
          {/each}
        </dl>
      </div>
    </section>
  {/if}
</Shell>
