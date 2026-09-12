# TT-046 Per-Structure Memory Accounting

TT-046 adds a read-only memory breakdown for the native HAT-trie and its typed
Go backing pools. It is inspired by Tarantool slab and memory accounting: the
operator can identify which storage family retains capacity without changing
the cache representation or enabling a background worker.

## API

Call `HatTrie.MemoryAccounting()` from Go, or request:

```text
GET /api/memory/structures
```

The response contains:

- `native_trie_bytes`: requested bytes reported by the native trie;
- `total_backing_bytes`: the sum of all returned structure rows;
- `total_bytes`: native plus backing bytes;
- `structures`: deterministic rows with `name` and `backing_bytes`.

The rows are:

```text
strings, raws, disks, maps, map_small, slices, slice_one_values,
slice_two_values, sets, set_one_strings, set_two_strings, priority_queues,
bloom_filters, count_min_sketches, hyper_log_logs, top_ks, cuckoo_filters,
roaring_bitmaps, quantile_sketches, fenwick_trees, sparse_bitsets,
reservoir_samples, xor_filters, radix_trees, dbrefs, replication_merkle
```

When local in-process partitions are enabled, the report aggregates all child
tries into one logical-cache result. The estimates exclude allocator metadata
and nested payloads referenced by entries, so they are intended for comparing
retained backing capacity and finding growth hotspots, not as an RSS reading.

## Metrics

The existing `/metrics` endpoint exposes:

```text
hatrie_cache_native_trie_bytes
hatrie_cache_backing_bytes
hatrie_cache_total_owned_bytes
hatrie_cache_structure_backing_bytes{structure="..."}
```

The detailed snapshot allocates one 640-byte row slice in the measured
fixture. Normal cache mutations do not call this API, and the existing
zero-allocation compaction accounting path remains unchanged.
