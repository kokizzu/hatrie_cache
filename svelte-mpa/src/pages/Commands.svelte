<script lang="ts">
  import { Save, Send, TimerReset } from '@lucide/svelte';
  import Shell from '../components/Shell.svelte';
  import { runCommand } from '../lib/api';

  let command = 'SETSTR';
  let key = '';
  let value = '';
  let ttl = 3600;
  let persist = false;
  let response = '';

  async function submit() {
    const result = await runCommand({
      command,
      key,
      value,
      ttl_seconds: persist ? null : ttl
    });
    response = result.value ? `${result.message} ${result.value}` : result.message;
  }
</script>

<Shell active="commands">
  <header class="page-header">
    <div>
      <p>Management</p>
      <h1>Commands</h1>
    </div>
  </header>

  <section class="command-layout">
    <form class="panel command-form" on:submit|preventDefault={submit}>
      <div class="panel-heading">
        <div>
          <h2>Run Command</h2>
          <p>Send a single cache operation</p>
        </div>
        <Send size={18} aria-hidden="true" />
      </div>

      <label>
        <span>Command</span>
        <select bind:value={command}>
          <option>GET</option>
          <option>SETSTR</option>
          <option>SETINT</option>
          <option>DEL</option>
          <option>EXPIRE</option>
        </select>
      </label>

      <label>
        <span>Key</span>
        <input bind:value={key} placeholder="session:user:1024" required />
      </label>

      {#if command !== 'GET' && command !== 'DEL' && command !== 'EXPIRE'}
        <label>
          <span>Value</span>
          <textarea bind:value={value} rows="4" placeholder="value"></textarea>
        </label>
      {/if}

      {#if command !== 'GET' && command !== 'DEL'}
        <div class="ttl-row">
          <label>
            <span>TTL seconds</span>
            <input type="number" min="1" bind:value={ttl} disabled={persist} />
          </label>
          <label class="checkbox-row">
            <input type="checkbox" bind:checked={persist} />
            <span>Persistent</span>
          </label>
        </div>
      {/if}

      <button class="primary-button" type="submit">
        <Save size={17} aria-hidden="true" />
        Execute
      </button>
    </form>

    <section class="panel">
      <div class="panel-heading">
        <div>
          <h2>Response</h2>
          <p>Last command result</p>
        </div>
        <TimerReset size={18} aria-hidden="true" />
      </div>
      <pre class="response-box">{response || 'No command has been executed in this session.'}</pre>
    </section>
  </section>
</Shell>
