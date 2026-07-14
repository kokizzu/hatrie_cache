<script lang="ts">
  import { onMount } from 'svelte';
  import { Clock3, Database, HardDrive, RefreshCw, ShieldCheck, TrendingUp } from '@lucide/svelte';
  import Shell from '../components/Shell.svelte';
  import StatTile from '../components/StatTile.svelte';
  import StatusBadge from '../components/StatusBadge.svelte';
  import MiniBarChart from '../components/MiniBarChart.svelte';
  import { DEFAULT_ENTRIES_LIMIT, loadEntries, loadHealth, loadStats, type CacheEntry, type CacheHealth, type CacheStats } from '../lib/api';
  import { formatBytes, formatRate, formatRelativeTime, formatTTL } from '../lib/format';

  let health: CacheHealth | null = null;
  let stats: CacheStats | null = null;
  let entries: CacheEntry[] = [];
  let entriesHasMore = false;
  let loading = true;

  async function refresh() {
    loading = true;
    const [nextHealth, nextStats, nextEntries] = await Promise.all([loadHealth(), loadStats(), loadEntries('', DEFAULT_ENTRIES_LIMIT)]);
    health = nextHealth;
    stats = nextStats;
    entries = nextEntries.entries;
    entriesHasMore = Boolean(nextEntries.has_more);
    loading = false;
  }

  onMount(refresh);

  $: operations = stats ? [stats.reads, stats.writes, stats.deletes, stats.expirations] : [];
  $: topEntries = [...entries].sort((a, b) => b.size_bytes - a.size_bytes).slice(0, 5);
</script>

<Shell active="dashboard">
  <header class="page-header">
    <div>
      <p>Monitoring</p>
      <h1>Dashboard</h1>
    </div>
    <button class="icon-button" type="button" on:click={refresh} aria-label="Refresh dashboard" title="Refresh dashboard">
      <RefreshCw size={18} class={loading ? 'spin' : ''} />
    </button>
  </header>

  {#if health && stats}
    <section class="stats-grid">
      <StatTile label="Hit rate" value={formatRate(stats.hit_rate)} detail={`${stats.hits.toLocaleString()} hits`} tone="green" icon={TrendingUp} />
      <StatTile label="Reads" value={stats.reads.toLocaleString()} detail={`${stats.misses.toLocaleString()} misses`} tone="blue" icon={Database} />
      <StatTile label="Memory" value={formatBytes(health.memory_bytes)} detail={`${formatBytes(health.disk_spill_bytes)} spilled`} tone="amber" icon={HardDrive} />
      <StatTile label="Cleaner" value={`${health.cleaners_running}`} detail={`uptime ${Math.floor(health.uptime_seconds / 3600)}h`} tone="blue" icon={ShieldCheck} />
    </section>

    <section class="dashboard-grid">
      <div class="panel">
        <div class="panel-heading">
          <div>
            <h2>Node Health</h2>
            <p>{health.node}</p>
          </div>
          <StatusBadge status={health.status} />
        </div>
        <dl class="detail-list">
          <div><dt>Last hit</dt><dd>{formatRelativeTime(stats.last_hit)}</dd></div>
          <div><dt>Last miss</dt><dd>{formatRelativeTime(stats.last_miss)}</dd></div>
          <div><dt>Last write</dt><dd>{formatRelativeTime(stats.last_write)}</dd></div>
          <div><dt>Cumulative hit rate</dt><dd>{formatRate(stats.cumulative_hit_rate)}</dd></div>
        </dl>
      </div>

      <div class="panel">
        <div class="panel-heading">
          <div>
            <h2>Operation Volume</h2>
            <p>Reads, writes, deletes, expirations</p>
          </div>
          <Clock3 size={18} aria-hidden="true" />
        </div>
        <MiniBarChart values={operations} labels={['Reads', 'Writes', 'Deletes', 'Expired']} />
      </div>
    </section>

    <section class="panel">
      <div class="panel-heading">
        <div>
          <h2>Largest Loaded Keys</h2>
          <p>{entriesHasMore ? `${entries.length.toLocaleString()} loaded, more available` : 'By stored value size'}</p>
        </div>
        <a class="text-link" href="/keys.html">View keys</a>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>Key</th><th>Type</th><th>TTL</th><th>Storage</th><th>Size</th></tr>
          </thead>
          <tbody>
            {#each topEntries as entry}
              <tr>
                <td><code>{entry.key}</code></td>
                <td>{entry.type}</td>
                <td>{formatTTL(entry.ttl_ms)}</td>
                <td>{entry.on_disk ? 'disk' : 'memory'}</td>
                <td>{formatBytes(entry.size_bytes)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </section>
  {/if}
</Shell>
