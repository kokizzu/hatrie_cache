<script lang="ts">
  import { Activity, Database, Settings, TerminalSquare } from '@lucide/svelte';

  export let active: 'dashboard' | 'keys' | 'commands' | 'admin';

  const nav = [
    { id: 'dashboard', href: '/', label: 'Dashboard', icon: Activity },
    { id: 'keys', href: '/keys.html', label: 'Keys', icon: Database },
    { id: 'commands', href: '/commands.html', label: 'Commands', icon: TerminalSquare },
    { id: 'admin', href: '/admin.html', label: 'Admin', icon: Settings }
  ] as const;
</script>

<div class="app-shell">
  <aside class="sidebar">
    <a class="brand" href="/">
      <span class="brand-mark">HC</span>
      <span>
        <strong>HATrie Cache</strong>
        <small>Management</small>
      </span>
    </a>

    <nav aria-label="Primary">
      {#each nav as item}
        <a class:active={active === item.id} href={item.href}>
          <svelte:component this={item.icon} size={18} aria-hidden="true" />
          <span>{item.label}</span>
        </a>
      {/each}
    </nav>
  </aside>

  <main class="content">
    <slot />
  </main>
</div>
