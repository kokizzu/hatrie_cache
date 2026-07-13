<script lang="ts">
  export let values: number[] = [];
  export let labels: string[] = [];

  $: max = Math.max(...values, 1);
</script>

<div class="mini-bars" role="img" aria-label="Cache operation volume">
  {#each values as value, index}
    <div class="mini-bar-row">
      <span>{labels[index]}</span>
      <div>
        <i style={`width: ${(value / max) * 100}%`}></i>
      </div>
      <strong>{value.toLocaleString()}</strong>
    </div>
  {/each}
</div>
