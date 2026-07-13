<script lang="ts">
  import type { Component } from 'svelte';

  export let label: string;
  export let value: string;
  export let detail = '';
  export let tone: 'blue' | 'green' | 'amber' | 'red' = 'blue';
  export let icon: Component;
</script>

<section class={`stat-tile ${tone}`}>
  <div>
    <span>{label}</span>
    <strong>{value}</strong>
    {#if detail}
      <small>{detail}</small>
    {/if}
  </div>
  <svelte:component this={icon} size={24} aria-hidden="true" />
</section>
