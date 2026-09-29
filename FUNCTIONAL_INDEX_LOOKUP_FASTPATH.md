# Functional Index Lookup Fast Path

This change applies a small index-arrangement optimization inspired by the
tight posting-list traversal used by columnar engines such as ClickHouse and
typed secondary indexes such as Tarantool's: once a posting list is under the
index's private write lock, each ID is known to have a live entry. The lookup
path therefore reads the value directly instead of repeating a defensive map
existence branch for every hit.

The invariant is maintained by `Upsert`, key moves, `Delete`, and `Clear`; the
posting and entry maps are private and all mutations use the same lock. No API,
ordering, storage format, or default behavior changed. The caller-owned
scratch path still performs zero allocations.

## Measurement

Command: `make benchmark-functional-index`

Workload: repeated `LookupInto` with a preallocated destination and 1, 8, or
128 hits. Five samples per case, AMD Ryzen 9 5950X, Go benchmark `-benchmem`.

| Posting hits | Before (ns/op, five samples) | After (ns/op, five samples) | Median change | Bytes/op | Allocs/op |
| ---: | --- | --- | ---: | ---: | ---: |
| 1 | 11.05, 10.26, 11.26, 10.26, 11.46 | 11.06, 10.92, 10.93, 11.44, 10.44 | 1.1% faster | 0 | 0 |
| 8 | 43.97, 46.49, 47.94, 45.76, 46.74 | 38.59, 39.10, 44.68, 42.82, 40.71 | 12.4% faster | 0 | 0 |
| 128 | 895.0, 962.5, 943.6, 906.8, 945.5 | 929.1, 902.9, 867.4, 1024, 943.7 | 1.5% faster | 0 | 0 |

The result is a CPU-only win on the measured workloads: it adds no memory,
allocation, locking, or compatibility cost. The 128-hit result is effectively
noise-level improvement, so this is intentionally kept as a narrow fast path,
not presented as a broad index redesign.

## Verification

- `make test-functional-index`
- `make race-functional-index`
- `make format-functional-index`
- The mixed lifecycle regression covers inserts, deletes, key moves, and
  reinserted IDs before the lookup implementation change.

The full package target was also attempted, but the current shared worktree
has unrelated parallel-session references to missing aggregate-state symbols
(`validateAggregateStateMetadata` and `AggregateStateEnvelope`).
