# CH-012: Bitmap-Backed Lightweight Logical Deletes

Status: implemented as an opt-in in-memory optimization.

## Why

Typed-table patch parts keep physical row positions while a logical delete is
waiting for threshold-based compaction. The old tombstone representation used
one Go `bool` per physical row. CH-012 stores the same state as one bit per row
and keeps the existing compaction threshold and locking behavior.

`TypedTablePatchOptions.Enabled` remains `false` by default. Existing physical
delete behavior is unchanged, and enabling patch parts does not change the
public row or mutation semantics.

## Representation

`typedTableDeleteBitmap` stores 64 rows in each `uint64` word. Insert and
reinsert clear a bit, logical delete sets a bit, and physical compaction tests
the bit before copying a live row. The bitmap is truncated with the compacted
row count after a successful rewrite.

When at least 64 rows are deleted, the no-TTL `Rows` path classifies complete
bitmap words and visits only live rows. Empty or sparse masks retain the simple
per-row path, avoiding extra traversal work for the common small-delete case.

This is an in-memory representation change only. It adds no storage-file or
wire-format migration and uses the existing patch-compaction scheduler; it
does not start a new background worker.

## Measurements

Commands:

```text
make benchmark-ch012-mask-c203
make benchmark-ch012-rows-c203
make benchmark-ch012-c203
```

The values below are medians of five benchmark samples from the same local
build. CPU numbers vary by host; allocation and retained backing comparisons
are the important memory result.

### Delete-mask backing

| Physical rows | `[]bool` B/op | Bitmap B/op | Backing reduction |
| ---: | ---: | ---: | ---: |
| 1,024 | 1,024 | 128 | 8.00x less |
| 10,000 | 10,240 | 1,280 | 8.00x less |
| 100,000 | 106,496 | 13,568 | 7.85x less |

Both representations use one allocation in this isolated benchmark.

### End-to-end typed-table path

| Workload | Baseline | Bitmap | Result |
| --- | ---: | ---: | --- |
| Logical delete/reinsert CPU | 820.5 ns/op | 691.7 ns/op | 15.7% lower |
| Logical delete/reinsert memory | 1,156 B/op, 4 allocs/op | 1,126 B/op, 4 allocs/op | allocations unchanged |
| `Rows` after half the rows are logically deleted | 913,811 ns/op | 903,619 ns/op | 1.1% lower |
| `Rows` after half-delete memory | 1,999,941 B/op, 19,873 allocs/op | 1,999,941 B/op, 19,873 allocs/op | unchanged |
| Patch compaction, 1,000 rows | 32,381 ns/op | 31,994 ns/op | 1.2% lower |
| Patch compaction, 10,000 rows | 522,120 ns/op | 455,353 ns/op | 12.8% lower |

Both compaction variants remain at zero benchmark allocations. The compaction
CPU differences are small-workload measurements and should not be treated as
a guaranteed speedup; the durable win is the bitmap backing reduction, while
the optimized dense-delete scan avoids the initial per-row bitmap regression.

### Raw mask samples

```text
rows=1024:   bool 245.0, 235.4, 252.3, 256.2, 255.5 ns/op; 1024 B/op
             bitmap 44.67, 45.21, 44.67, 46.06, 46.13 ns/op; 128 B/op
rows=10000:  bool 1412, 1546, 1511, 1551, 1479 ns/op; 10240 B/op
             bitmap 323.1, 299.6, 340.4, 294.2, 296.4 ns/op; 1280 B/op
rows=100000: bool 11979, 12700, 12654, 12636, 12950 ns/op; 106496 B/op
             bitmap 2458, 2323, 2376, 2296, 2281 ns/op; 13568 B/op
```

## Correctness and fallback

Focused tests cover word packing, setting, clearing, truncation, logical
delete visibility, reinsertion, and compaction. The full `hatSql` test, race,
vet, and diff checks pass.

There is no runtime format fallback because the bitmap is internal and
backward-compatible. Disable `TypedTablePatchOptions.Enabled` to retain the
physical-delete path. If a workload has very sparse logical deletes, the
representation still uses compact backing and keeps the existing simple scan
until the dense-delete traversal threshold is reached.
