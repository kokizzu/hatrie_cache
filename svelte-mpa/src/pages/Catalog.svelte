<script lang="ts">
  import { onMount } from 'svelte';
  import { Database, RefreshCw, TableProperties } from '@lucide/svelte';
  import Shell from '../components/Shell.svelte';
  import { loadSQLCatalog, type SQLCatalog } from '../lib/api';

  let catalog: SQLCatalog = { namespaces: [], schema: { version: 0, sources: {} }, indexes: [] };
  let loading = true;
  let error = '';

  async function refresh() {
    loading = true;
    error = '';
    try {
      catalog = await loadSQLCatalog();
    } catch (reason) {
      error = reason instanceof Error ? reason.message : 'Catalog request failed.';
    } finally {
      loading = false;
    }
  }

  onMount(refresh);
  $: sources = Object.values(catalog.schema.sources ?? {}).sort((left, right) => left.name.localeCompare(right.name));
  $: namespaces = [...catalog.namespaces].sort((left, right) => left.localeCompare(right));
  $: indexes = [...catalog.indexes].sort((left, right) => left.source.localeCompare(right.source) || left.name.localeCompare(right.name));
</script>

<Shell active="catalog">
  <header class="page-header">
    <div><p>SQL Metadata</p><h1>Catalog</h1></div>
    <button class="icon-button" type="button" on:click={refresh} aria-label="Refresh catalog" title="Refresh catalog"><RefreshCw size={18} class={loading ? 'spin' : ''} /></button>
  </header>

  {#if error}<p class="notice danger-notice">{error}</p>{/if}

  <section class="panel">
    <div class="panel-heading"><div><h2>Namespaces</h2><p>{namespaces.length.toLocaleString()} declared namespaces</p></div><Database size={18} aria-hidden="true" /></div>
    {#if namespaces.length}
      <div class="catalog-tags">{#each namespaces as namespace}<code>{namespace}</code>{/each}</div>
    {:else}
      <p class="empty-state">No namespaces are declared.</p>
    {/if}
  </section>

  <section class="panel catalog-panel">
    <div class="panel-heading"><div><h2>Schemas</h2><p>Version {catalog.schema.version}</p></div><TableProperties size={18} aria-hidden="true" /></div>
    {#if sources.length}
      <div class="table-wrap"><table><thead><tr><th>Source</th><th>Column</th><th>Type</th><th>Required</th></tr></thead><tbody>
        {#each sources as source}
          {#each source.columns as column, index}
            <tr><td>{index === 0 ? source.name : ''}</td><td><code>{column.name}</code></td><td>{column.type}</td><td>{column.not_null ? 'yes' : 'no'}</td></tr>
          {:else}
            <tr><td>{source.name}</td><td colspan="3">No declared columns</td></tr>
          {/each}
        {/each}
      </tbody></table></div>
    {:else}
      <p class="empty-state">No source schemas are declared.</p>
    {/if}
  </section>

  <section class="panel catalog-panel">
    <div class="panel-heading"><div><h2>Indexes</h2><p>{indexes.length.toLocaleString()} declared indexes</p></div><TableProperties size={18} aria-hidden="true" /></div>
    {#if indexes.length}
      <div class="table-wrap"><table><thead><tr><th>Source</th><th>Index</th><th>Kind</th><th>Columns</th></tr></thead><tbody>{#each indexes as index}<tr><td>{index.source}</td><td><code>{index.name}</code></td><td>{index.kind}</td><td>{index.columns.join(', ')}</td></tr>{/each}</tbody></table></div>
    {:else}
      <p class="empty-state">No indexes are declared.</p>
    {/if}
  </section>
</Shell>
