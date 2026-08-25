<script lang="ts">
  import { BarChart3, History, Play, RotateCcw } from '@lucide/svelte';
  import Shell from '../components/Shell.svelte';
  import { runSQL, type SQLQueryResult } from '../lib/api';

  type HistoryItem = { query: string; elapsed: number; rows: number; ok: boolean; at: string };
  const historyKey = 'hatrie-cache.sql-history';
  let query = "FROM KEYS SELECT key, type, size_bytes ORDER BY key LIMIT 100";
  let parameters = '[]';
  let analyze = true;
  let running = false;
  let error = '';
  let result: SQLQueryResult | null = null;
  let history: HistoryItem[] = [];

  try { history = JSON.parse(sessionStorage.getItem(historyKey) ?? '[]'); } catch { history = []; }

  function highlight(source: string) {
    return source.replace(/\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|ON|GROUP|BY|ORDER|LIMIT|EXPLAIN|ANALYZE|AS|AND|OR|COUNT|SUM|AVG)\b/gi, '<mark>$1</mark>');
  }
  function save(item: HistoryItem) {
    history = [item, ...history.filter((entry) => entry.query !== item.query)].slice(0, 20);
    sessionStorage.setItem(historyKey, JSON.stringify(history));
  }
  async function execute() {
    error = ''; result = null; running = true;
    const started = performance.now();
    try {
      const parsed = JSON.parse(parameters);
      if (!Array.isArray(parsed)) throw new Error('Parameters must be a JSON array.');
      const source = analyze && !/^\s*EXPLAIN\b/i.test(query) ? `EXPLAIN ANALYZE ${query}` : query;
      result = await runSQL(source, parsed);
      save({ query, elapsed: performance.now() - started, rows: result.rows.length, ok: true, at: new Date().toISOString() });
    } catch (reason) {
      error = reason instanceof Error ? reason.message : 'SQL query failed.';
      save({ query, elapsed: performance.now() - started, rows: 0, ok: false, at: new Date().toISOString() });
    } finally { running = false; }
  }
  function reset() { query = ''; parameters = '[]'; result = null; error = ''; }
</script>

<Shell active="sql">
  <header class="page-header"><div><p>Read-only analysis</p><h1>SQL</h1></div></header>
  <section class="sql-layout">
    <section class="panel sql-editor">
      <div class="panel-heading"><div><h2>Query</h2><p>Snapshot query workspace</p></div><BarChart3 size={18} aria-hidden="true" /></div>
      <div class="sql-highlight" aria-hidden="true">{@html highlight(query)}</div>
      <textarea aria-label="SQL query" bind:value={query} spellcheck="false" rows="9"></textarea>
      <label><span>Parameters JSON</span><input bind:value={parameters} /></label>
      <div class="sql-actions"><label class="checkbox-row"><input type="checkbox" bind:checked={analyze} /><span>Explain analyze</span></label><button class="icon-button" type="button" title="Clear query" on:click={reset}><RotateCcw size={17}/></button><button class="primary-button" type="button" disabled={running || !query.trim()} on:click={execute}><Play size={17}/>{running ? 'Running' : 'Run'}</button></div>
      {#if error}<p class="sql-error">{error}</p>{/if}
    </section>
    <section class="panel sql-history"><div class="panel-heading"><div><h2>History</h2><p>Current browser session</p></div><History size={18}/></div>
      {#each history as item}<button class:failed={!item.ok} type="button" on:click={() => query = item.query}><strong>{item.ok ? `${item.rows} rows` : 'Failed'}</strong><span>{item.elapsed.toFixed(1)} ms</span><small>{item.query}</small></button>{:else}<p class="empty-state">No query history.</p>{/each}
    </section>
  </section>
  {#if result}
    <section class="panel sql-results"><div class="panel-heading"><div><h2>Results</h2><p>{result.rows.length} rows</p></div></div><div class="table-wrap"><table><thead><tr>{#each result.columns as column}<th>{column}</th>{/each}</tr></thead><tbody>{#each result.rows as row}<tr>{#each result.columns as column}<td>{typeof row[column] === 'object' ? JSON.stringify(row[column]) : String(row[column] ?? '')}</td>{/each}</tr>{/each}</tbody></table></div></section>
    {#if result.plan?.length}<section class="panel sql-plan"><div class="panel-heading"><div><h2>Execution plan</h2><p>Measured operators</p></div></div><div class="table-wrap"><table><thead><tr><th>Operator</th><th>Detail</th><th>Estimate</th><th>Output</th><th>Elapsed</th></tr></thead><tbody>{#each result.plan as step}<tr><td>{step.node}</td><td>{step.detail}</td><td>{step.estimated_rows ?? '-'}</td><td>{step.actual_output_rows ?? '-'}</td><td>{step.elapsed_ns ? `${(step.elapsed_ns / 1e6).toFixed(2)} ms` : '-'}</td></tr>{/each}</tbody></table></div></section>{/if}
  {/if}
</Shell>
