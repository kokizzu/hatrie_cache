<script lang="ts">
  export let status: 'online' | 'degraded' | 'offline' | string;
</script>

<span class={`status-badge ${status}`}>{status}</span>
