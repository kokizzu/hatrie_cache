# Data-Structure HAT-Trie Split Proposal

## Current design

The cache has **one shared C HAT-trie** for the global key namespace. Each
entry is a compact five-byte `HatValue`: an inline counter or an index plus a
type tag into **typed backing pools**. Those pools store strings, bytes, maps,
deques, sets, queues, filters, sketches, bitmaps, indexes, and persistence
references.

This is intentionally not one HAT-trie per value type. Keys are stored once;
the directory identifies the active type and the backing pool owns only its
payload. TTL and disk state are flags on the compact record. See
[DATA_STRUCTURE.md](DATA_STRUCTURE.md) for the supported types and commands.

## Proposal evaluated

Create a separate HAT-trie for every value family, such as a string trie, set
trie, bitmap trie, and sketch trie. A key would live in the trie matching its
current type.

## Potential benefits

| Benefit | When it matters |
| --- | --- |
| Reduced lock contention | Workloads are highly concurrent and permanently type-isolated. |
| Independent quotas/eviction | Operators need different retention rules per family. |
| Per-type rebuilds | One family has exceptional churn and needs isolated compaction. |
| Type-scoped scans | Callers only scan one declared family. |

## Costs and risks

| Cost | Effect |
| --- | --- |
| Duplicate key storage | Each HAT-trie needs key bytes and nodes; the present directory stores each key once. |
| Cross-type replacement | Replacing `string:k` with `set:k` becomes an atomic move between tries instead of one record replacement. |
| Generic command cost | `GET`, `EXISTS`, `DEL`, `TTL`, `DUMP`, and untyped scans need many lookups or another global index. |
| Prefix cost | A global prefix result needs scanning, merging, and ordering every trie. |
| Lock complexity | Multi-trie writes, expiration, backup, and replication require a strict lock order. |
| Backup and restore complexity | Consistent backup must coordinate every trie and preserve cross-type overwrite order. |
| Replication complexity | Journal replay and anti-entropy need type-aware routing plus atomic cross-type transitions. |
| Small memory saving | Removing a type tag saves little; typed backing pools and values remain. |

Type is mutable, while keys, TTL, backup, replication, and generic commands are
global. That makes the normal cache contract a poor fit for a per-type split.

## Recommendation

**Do not split the internal HAT-trie by data structure.** Keep one shared
C HAT-trie plus typed backing pools as the default and only implementation. It
preserves one authoritative directory, compact memory use, and simple atomic
replacement semantics.

For performance work, measure the actual contention point first:

1. Add narrowly scoped read/write fast paths or striped locking only when a
   benchmark demonstrates a net improvement.
2. Keep exact-key operations on the common directory so type replacement,
   expiration, and deletion remain atomic.
3. Use key-range/local partitions or separate `HatTrie` instances when an
   operator needs isolation. These boundaries are explicit and keep backup,
   restore, and replication understandable.

## When to reconsider

Reconsider only as an opt-in mode when all conditions hold:

- Keys are permanently type-owned; changing a type is rejected or an explicit
  migration.
- Generic commands and global prefix scans are absent or explicitly scoped to
  a single family.
- Backups, restores, replication, and TTL have a tested multi-trie consistency
  protocol.
- Benchmarks show a meaningful gain after including memory, tail latency,
  backup, and replication costs.

Until then, a split adds operational and correctness cost without a credible
default-path win.
