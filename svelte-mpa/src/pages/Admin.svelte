<script lang="ts">
  import { onMount } from 'svelte';
  import { Activity, Clock3, Database, HardDrive, RefreshCw, Send } from '@lucide/svelte';
  import Shell from '../components/Shell.svelte';
  import StatTile from '../components/StatTile.svelte';
  import {
    compactStorage,
    flushStorage,
    loadReplicationStatus,
    loadStorageStatus,
    syncReplication,
    type ReplicationResult,
    type ReplicationQueueStats,
    type StorageCompactResult,
    type StorageFlushResult,
    type StorageStatus
  } from '../lib/api';
  import { formatBytes, formatRelativeTime } from '../lib/format';

  let storage: StorageStatus | null = null;
  let replication: ReplicationResult | null = null;
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

  async function refresh() {
    loading = true;
    const [nextStorage, nextReplication] = await Promise.all([loadStorageStatus(), loadReplicationStatus()]);
    storage = nextStorage;
    replication = nextReplication;
    loading = false;
  }

  async function runFlush() {
    storageAction = 'flush';
    storageMessage = '';
    lastFlush = await flushStorage();
    storageMessage = `Flushed ${lastFlush.keys.toLocaleString()} keys in ${formatMillis(lastFlush.duration_millis)}.`;
    storageAction = '';
    await refresh();
  }

  async function runCompact() {
    storageAction = 'compact';
    storageMessage = '';
    lastCompact = await compactStorage(compactStartKey, compactLimitKey);
    storageMessage = `Compacted ${formatBytes(lastCompact.size_bytes_before)} to ${formatBytes(lastCompact.size_bytes_after)} in ${formatMillis(lastCompact.duration_millis)}.`;
    storageAction = '';
    await refresh();
  }

  async function runSync() {
    replicationAction = 'sync';
    replicationMessage = '';
    syncResult = await syncReplication(syncPrefix);
    replicationMessage = syncResult.skipped
      ? (syncResult.reason ?? 'Sync skipped.')
      : `Synced ${(syncResult.entries ?? 0).toLocaleString()} entries in ${formatMillis(syncResult.duration_millis)}.`;
    replicationAction = '';
    await refresh();
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

  function formatSignedBytes(value: number): string {
    if (value === 0) return '0 B';
    const sign = value > 0 ? '+' : '-';
    return `${sign}${formatBytes(Math.abs(value))}`;
  }

  function targetRows(values?: Record<string, number>): [string, number][] {
    return Object.entries(values ?? {}).sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
  }

  function compactPropertyText(result: StorageCompactResult | null): string {
    if (!result) return 'No compaction has run in this session.';
    const props = result.properties_after;
    return props.stats || props.sstables || props.write_delay || props.block_pool || 'No LevelDB properties reported.';
  }

  onMount(refresh);

  $: queue = replication?.queue;
  $: targets = replication?.targets ?? [];
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
    <StatTile label="LevelDB" value={storage?.leveldb_configured ? 'enabled' : 'off'} detail={lastFlush ? `${lastFlush.keys.toLocaleString()} keys flushed` : 'storage engine'} tone="blue" icon={Database} />
    <StatTile label="Queue" value={queue ? `${queue.depth}/${queue.capacity}` : 'off'} detail={queue?.closed ? 'closed' : queue?.enabled ? 'async enabled' : 'not configured'} tone="green" icon={Activity} />
    <StatTile label="Oldest" value={queue?.oldest_queued_age_millis ? formatMillis(queue.oldest_queued_age_millis) : 'none'} detail={queue?.oldest_queued_key ?? 'queued key'} tone="amber" icon={Clock3} />
    <StatTile label="Dropped" value={(queue?.dropped ?? 0).toLocaleString()} detail={`${(queue?.failures ?? 0).toLocaleString()} failures`} tone={queue?.dropped || queue?.failures ? 'red' : 'blue'} icon={HardDrive} />
  </section>

  <section class="admin-layout">
    <div class="panel">
      <div class="panel-heading">
        <div>
          <h2>LevelDB Storage</h2>
          <p>{storage?.leveldb_configured ? 'Configured' : 'Not configured'}</p>
        </div>
        <Database size={18} aria-hidden="true" />
      </div>

      {#if storageMessage}
        <p class="notice">{storageMessage}</p>
      {/if}

      <div class="action-grid">
        <button class="primary-button" type="button" on:click={runFlush} disabled={!storage?.leveldb_configured || Boolean(storageAction)}>
          <HardDrive size={17} aria-hidden="true" />
          Flush
        </button>

        <div class="compact-form">
          <label>
            <span>Start key</span>
            <input bind:value={compactStartKey} placeholder="alpha" disabled={!storage?.leveldb_configured || Boolean(storageAction)} />
          </label>
          <label>
            <span>Limit key</span>
            <input bind:value={compactLimitKey} placeholder="omega" disabled={!storage?.leveldb_configured || Boolean(storageAction)} />
          </label>
          <button class="secondary-button" type="button" on:click={runCompact} disabled={!storage?.leveldb_configured || Boolean(storageAction)}>
            <RefreshCw size={17} class={storageAction === 'compact' ? 'spin' : ''} aria-hidden="true" />
            Compact
          </button>
        </div>
      </div>

      <dl class="detail-list">
        <div><dt>Last flush</dt><dd>{lastFlush ? formatRelativeTime(lastFlush.finished_at) : 'never'}</dd></div>
        <div><dt>Flush duration</dt><dd>{lastFlush ? formatMillis(lastFlush.duration_millis) : '0 ms'}</dd></div>
        <div><dt>Compaction delta</dt><dd>{lastCompact ? formatSignedBytes(lastCompact.size_bytes_delta) : '0 B'}</dd></div>
        <div><dt>Compaction range</dt><dd>{lastCompact ? `${lastCompact.start_key || '*'} to ${lastCompact.limit_key || '*'}` : '*'}</dd></div>
      </dl>

      <pre class="property-box">{compactPropertyText(lastCompact)}</pre>
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
        <button class="primary-button" type="button" on:click={runSync} disabled={replicationAction === 'sync'}>
          <Send size={17} aria-hidden="true" />
          Sync
        </button>
      </div>

      <dl class="detail-list">
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
