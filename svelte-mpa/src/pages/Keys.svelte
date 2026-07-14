<script lang="ts">
  import { onMount } from 'svelte';
  import { HardDrive, RefreshCw, Search, Trash2 } from '@lucide/svelte';
  import Shell from '../components/Shell.svelte';
  import { DEFAULT_ENTRIES_LIMIT, loadEntries, runCommand, type CacheEntry } from '../lib/api';
  import { filterEntries, formatBytes, formatTTL } from '../lib/format';

  let entries: CacheEntry[] = [];
  let prefix = '';
  let entriesLimit = DEFAULT_ENTRIES_LIMIT;
  let loadedLimit = DEFAULT_ENTRIES_LIMIT;
  let hasMore = false;
  let query = '';
  let type = 'all';
  let message = '';
  let loading = true;

  async function refresh() {
    loading = true;
    const requestedLimit = Math.max(1, Math.floor(Number(entriesLimit) || DEFAULT_ENTRIES_LIMIT));
    entriesLimit = requestedLimit;
    const response = await loadEntries(prefix, requestedLimit);
    entries = response.entries;
    loadedLimit = response.limit ?? requestedLimit;
    hasMore = Boolean(response.has_more);
    loading = false;
  }

  async function removeKey(key: string) {
    const response = await runCommand({ command: 'DEL', key });
    message = response.message;
    entries = entries.filter((entry) => entry.key !== key);
  }

  onMount(refresh);

  $: filtered = filterEntries(entries, query, type);
</script>

<Shell active="keys">
  <header class="page-header">
    <div>
      <p>Inventory</p>
      <h1>Keys</h1>
    </div>
    <button class="icon-button" type="button" on:click={refresh} aria-label="Refresh keys" title="Refresh keys">
      <RefreshCw size={18} class={loading ? 'spin' : ''} />
    </button>
  </header>

  <section class="toolbar" aria-label="Key filters">
    <label>
      <span>Prefix</span>
      <input bind:value={prefix} placeholder="session:" on:change={refresh} />
    </label>
    <label class="search-field">
      <Search size={16} aria-hidden="true" />
      <input bind:value={query} placeholder="Search keys" />
    </label>
    <label>
      <span>Type</span>
      <select bind:value={type}>
        <option value="all">All</option>
        <option value="counter">Counter</option>
        <option value="string">String</option>
        <option value="bytes">Bytes</option>
        <option value="map">Map</option>
        <option value="slice">Slice</option>
        <option value="set">Set</option>
        <option value="priority_queue">Priority Queue</option>
        <option value="bloom_filter">Bloom Filter</option>
        <option value="xor_filter">XOR Filter</option>
        <option value="radix_tree">Radix Tree</option>
        <option value="count_min_sketch">Count-Min Sketch</option>
        <option value="hyperloglog">HyperLogLog</option>
        <option value="top_k">Top-K</option>
        <option value="quantile_sketch">Quantile Sketch</option>
        <option value="fenwick_tree">Fenwick Tree</option>
        <option value="reservoir_sample">Reservoir Sample</option>
        <option value="sparse_bitset">Sparse Bitset</option>
      </select>
    </label>
    <label>
      <span>Limit</span>
      <input type="number" min="1" max="100000" step="100" bind:value={entriesLimit} on:change={refresh} />
    </label>
  </section>

  {#if message}
    <p class="notice">{message}</p>
  {/if}

  <section class="panel">
    <div class="panel-heading">
      <div>
        <h2>{filtered.length.toLocaleString()} keys</h2>
        <p>
          Filtered from {entries.length.toLocaleString()} loaded entries{hasMore
            ? `, limited to ${loadedLimit.toLocaleString()} with more available`
            : ''}
        </p>
      </div>
      <HardDrive size={18} aria-hidden="true" />
    </div>

    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Key</th>
            <th>Type</th>
            <th>TTL</th>
            <th>Storage</th>
            <th>Size</th>
            <th>Preview</th>
            <th class="action-col">Action</th>
          </tr>
        </thead>
        <tbody>
          {#each filtered as entry}
            <tr>
              <td><code>{entry.key}</code></td>
              <td><span class={`type-pill ${entry.type}`}>{entry.type}</span></td>
              <td>{formatTTL(entry.ttl_ms)}</td>
              <td>{entry.on_disk ? 'disk' : 'memory'}</td>
              <td>{formatBytes(entry.size_bytes)}</td>
              <td class="preview">{entry.value_preview}</td>
              <td>
                <button class="table-button danger" type="button" on:click={() => removeKey(entry.key)} aria-label={`Delete ${entry.key}`} title="Delete key">
                  <Trash2 size={16} />
                </button>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>
</Shell>
