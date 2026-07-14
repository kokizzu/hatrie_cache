<script lang="ts">
  import { Save, Send, TimerReset } from '@lucide/svelte';
  import Shell from '../components/Shell.svelte';
  import { runCommand } from '../lib/api';

  let command = 'SETSTR';
  let key = '';
  let value = '';
  let subkey = '';
  let ttl = 3600;
  let priority = 0;
  let persist = false;
  let response = '';

  $: needsValue = !['GET', 'DEL', 'EXPIRE', 'PEEKPQ', 'POPPQ', 'GETPQ', 'INFOBF', 'INFOCF', 'BUILDXF', 'INFOXF', 'CREATERB', 'COUNTRB', 'GETRB', 'INFORB', 'CREATESB', 'COUNTSB', 'GETSB', 'INFOSB', 'CREATERT', 'GETRT', 'DELRT', 'HASRT', 'PREFIXRT', 'INFORT', 'INFOCMS', 'COUNTHLL', 'INFOHLL', 'GETTOPK', 'INFOTOPK', 'GETRS', 'INFORS', 'INFOQ', 'INFOFW'].includes(command);
  $: needsTTL = ['SETSTR', 'SETINT', 'EXPIRE'].includes(command);
  $: needsPriority = command === 'PUSHPQ';
  $: needsSubkey = ['CREATECF', 'CREATECMS', 'INCRCMS', 'ADDTOPK', 'ADDFW', 'RANGEFW', 'PUTRT', 'GETRT', 'DELRT', 'HASRT', 'PREFIXRT'].includes(command);
  $: subkeyLabel = command === 'CREATECMS' ? 'Depth' : command === 'CREATECF' ? 'False positive rate' : command === 'ADDFW' ? 'Delta' : command === 'RANGEFW' ? 'End index' : command === 'PREFIXRT' ? 'Prefix' : ['PUTRT', 'GETRT', 'DELRT', 'HASRT'].includes(command) ? 'Nested key' : 'Count';
  $: valueLabel = command === 'CREATEFW' ? 'Size' : command === 'CREATERS' || command === 'CREATEXF' ? 'Capacity' : ['ADDFW', 'GETFW', 'SUMFW'].includes(command) ? 'Index' : command === 'RANGEFW' ? 'Start index' : 'Value';
  $: valuePlaceholder = command === 'CREATEFW' ? '1024' : command === 'CREATERS' ? '128' : command === 'CREATEXF' ? '10000' : ['ADDFW', 'GETFW', 'SUMFW', 'RANGEFW'].includes(command) ? '0' : 'value';
  $: subkeyPlaceholder = command === 'CREATECMS' ? '4' : command === 'CREATECF' ? '0.01' : command === 'RANGEFW' ? '10' : command === 'PREFIXRT' ? 'user:' : ['PUTRT', 'GETRT', 'DELRT', 'HASRT'].includes(command) ? 'user:100/profile' : '1';

  async function submit() {
    const result = await runCommand({
      command,
      key,
      value,
      subkey: needsSubkey ? subkey : undefined,
      priority: needsPriority ? priority : null,
      ttl_seconds: needsTTL && !persist ? ttl : null
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
          <option>PUSHPQ</option>
          <option>PEEKPQ</option>
          <option>POPPQ</option>
          <option>GETPQ</option>
          <option>CREATEBF</option>
          <option>ADDBF</option>
          <option>HASBF</option>
          <option>INFOBF</option>
          <option>CREATECF</option>
          <option>ADDCF</option>
          <option>HASCF</option>
          <option>DELCF</option>
          <option>INFOCF</option>
          <option>CREATEXF</option>
          <option>ADDXF</option>
          <option>BUILDXF</option>
          <option>HASXF</option>
          <option>INFOXF</option>
          <option>CREATERB</option>
          <option>ADDRB</option>
          <option>REMRB</option>
          <option>HASRB</option>
          <option>COUNTRB</option>
          <option>GETRB</option>
          <option>INFORB</option>
          <option>CREATESB</option>
          <option>ADDSB</option>
          <option>REMSB</option>
          <option>HASSB</option>
          <option>COUNTSB</option>
          <option>GETSB</option>
          <option>INFOSB</option>
          <option>CREATERT</option>
          <option>PUTRT</option>
          <option>GETRT</option>
          <option>DELRT</option>
          <option>HASRT</option>
          <option>PREFIXRT</option>
          <option>INFORT</option>
          <option>CREATECMS</option>
          <option>INCRCMS</option>
          <option>ESTCMS</option>
          <option>INFOCMS</option>
          <option>CREATEHLL</option>
          <option>ADDHLL</option>
          <option>COUNTHLL</option>
          <option>INFOHLL</option>
          <option>CREATETOPK</option>
          <option>ADDTOPK</option>
          <option>ESTTOPK</option>
          <option>GETTOPK</option>
          <option>INFOTOPK</option>
          <option>CREATERS</option>
          <option>ADDRS</option>
          <option>GETRS</option>
          <option>INFORS</option>
          <option>CREATEQ</option>
          <option>ADDQ</option>
          <option>ESTQ</option>
          <option>INFOQ</option>
          <option>CREATEFW</option>
          <option>ADDFW</option>
          <option>GETFW</option>
          <option>SUMFW</option>
          <option>RANGEFW</option>
          <option>INFOFW</option>
        </select>
      </label>

      <label>
        <span>Key</span>
        <input bind:value={key} placeholder="session:user:1024" required />
      </label>

      {#if needsValue}
        <label>
          <span>{valueLabel}</span>
          <textarea bind:value={value} rows="4" placeholder={valuePlaceholder}></textarea>
        </label>
      {/if}

      {#if needsPriority}
        <label>
          <span>Priority</span>
          <input type="number" bind:value={priority} />
        </label>
      {/if}

      {#if needsSubkey}
        <label>
          <span>{subkeyLabel}</span>
          <input bind:value={subkey} placeholder={subkeyPlaceholder} />
        </label>
      {/if}

      {#if needsTTL}
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
