# Typed-table Decompressed Block Cache

The typed-table columnar cache can optionally retain decoded scalar blocks after
the compressed batch has been built. This is a ClickHouse-inspired read cache
for workloads that repeatedly inspect the same compressed columns. It is
disabled by default and is never part of table persistence or backup data.

## Enable

The feature requires both the existing immutable layout cache and compressed
batches:

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
	Name: "events",
	Columns: []hatSql.TypedTableColumn{
		{Name: "id", Kind: hatSql.TypedTableInt64},
		{Name: "active", Kind: hatSql.TypedTableBool},
	},
	ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
		Enabled:                true,
		CompressedBatches:      true,
		DecompressedBlockCache: true,
	},
})
```

The options are normalized to these defaults when enabled:

| Option | Default | Meaning |
| --- | ---: | --- |
| `DecompressedBlockCache` | `false` | Opt-in switch |
| `DecompressedBlockMaxBytes` | `1 MiB` | Approximate resident decoded-block budget per cached layout |
| `DecompressedBlockRows` | `256` | Logical rows in one decoded block |
| `DecompressedBlockMinReads` | `2` | Misses required before a block is admitted |

When `Enabled` is false, `CompressedBatches` is false, or the feature switch
is false, the three numeric options are cleared to zero and no cache is
allocated.

## Behavior

- Dictionary, nullable-packed, bit-packed boolean, and fixed-width numeric
  columns can be cached.
- Plain, list, and nested columns use the existing uncached path.
- Each cached block is keyed by field and row block, and stores the exact
  logical values returned by `ColumnarBatch.Value`.
- Admission and bounded eviction use a mutex only on misses. Once published,
  block reads use per-column atomic pointer slots and do not lock.
- A mutation clears the immutable layout cache, so decoded blocks cannot outlive
  the source snapshot they represent.
- The cache is process memory only. Restore and restart rebuild it on demand.

Use the feature for repeated scans, duplicate projections, or repeated queries
over a warm immutable layout. Leave it disabled for cold, highly selective
lookups or tight-memory deployments; those workloads may not recover the
admission and lookup overhead.

## Measurement

The reproducible benchmark is `make benchmark-ch007`. It compares the same
65,536-row compressed numeric table and repeated duplicate projection with the
cache disabled and enabled. The measured enabled case was about 1.17x faster
than disabled, with about half the allocations and 1,073,152 bytes of retained
decoded blocks. The full raw samples and tradeoff table are in
[BENCHMARK.md](BENCHMARK.md#ch-007-decompressed-column-block-cache-with-admission).
