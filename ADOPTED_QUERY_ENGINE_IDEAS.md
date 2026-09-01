# Query Engine Ideas: Adoption Status

This matrix records the ClickHouse, Materialize, and Tarantool ideas assessed
for `hatrie_cache`. An idea is adopted only when it preserves exact query or
recovery behavior and either improves a measured workload or supplies an
explicitly opt-in operational control.

| Source | Idea | Status | Evidence |
|---|---|---|---|
| Materialize | Coordinated progress frontier | Adopted | `SQLProjectionRetentionFrontier` commits journal retention only after all configured runners succeed. [PROJECTION_FRONTIERS.md](PROJECTION_FRONTIERS.md) |
| Materialize | Shared arrangements | Adopted | `TypedTableAggregateArrangements` shares exact aggregate state among identical definitions, and `TypedTableJoinArrangements` shares exact incremental equi-joins. [TYPED_TABLE_ARRANGEMENTS.md](TYPED_TABLE_ARRANGEMENTS.md), [TYPED_TABLES.md](TYPED_TABLES.md) |
| ClickHouse | Projection for repeated ordering | Adopted | Immutable cached ordinal projections serve repeated single-column and multi-column `ORDER BY` Top-N queries, including mixed directions, after repeated reads. |
| ClickHouse | Dynamic Top-N data skipping | Adopted | Cached numeric segment min/max metadata skips only segments that cannot beat the bounded Top-N threshold; equal-boundary segments remain scanned to preserve stable ties. |
| ClickHouse | Granular skipping indexes | Already present | Numeric min/max, dictionary membership, string equality Bloom, and n-gram Bloom sidecars prune only impossible segments. |
| ClickHouse | Narrow expression index | Adopted | `CreateSQLJSONLowerIndex` is an explicit `LOWER(field) = literal` equality index. It uses the existing generation-aware JSON snapshot lifecycle and falls back to the SQL scan for source values that are not strings. |
| Tarantool | Controlled cooperative maintenance | Adopted | `ManagedRefreshScheduler` now has opt-in count and duration cycle budgets. [REFRESH_SCHEDULER.md](REFRESH_SCHEDULER.md) |
| Tarantool | Consistent read snapshot | Already present at current boundary | `SnapshotLocker` gives resolvers a stable query lifetime without retaining historical row versions. |

## Measured Results

| Feature | Result |
|---|---|
| Shared typed aggregate arrangement, two consumers over 10,000 changes | 2.02x faster, 1.98x less heap, and 1.99x fewer allocations than two independent aggregates. |
| Shared typed equi-join arrangement, one updated row with 10,000 rows per side and 64 string join keys | About 16.4 us, 1.0 KB, and 5 allocations, versus a 1.53 s, 634 MB, 3,175,006-allocation full rebuild: about 93,000x faster, 634,000x less allocated heap, and 635,000x fewer allocations. Factorized structural pairs retain source keys without cloning row values or serializing pair identities; same-kind string keys avoid a redundant prefix. It is explicit because retained state grows with matching-pair cardinality. |
| Typed equi-join coalesced same-key changes, two updates and 1,024 matches | About 2.56 us, 344 B, and 6 allocations, versus separate updates at 197 us, 192 KB, and 32 allocations: about 77x faster, 557x less allocated heap, and 5.3x fewer allocations. It preserves ordered validation and checkpoint-prefix behavior. |
| Projection retention frontier commit | 207 ns median, 0 B/op, 0 allocs/op; background-only and opt-in. |
| Refresh scheduler count budget | No measurable overhead over the default scheduler in the no-op refresh benchmark. |
| Refresh scheduler duration budget | About 568 ns, 352 B, and 5 allocations per opt-in no-op cycle due to the cooperative timeout context. |
| Composite columnar sorted projection, 20,000 rows and `LIMIT 50` | About 15.9 us, 21 KB, and 131 allocations when warm, versus about 2.30 ms, 986 KB, and 60,014 allocations for repeated heap Top-N: about 145x faster, 46x less heap, and 458x fewer allocations. The admitted index costs one 4-byte row ordinal per cached composite order, within the existing 4 MB layout-cache bound. |
| Mixed-direction composite columnar sorted projection, 20,000 rows and `LIMIT 50` | About 26.1 us, 21 KB, and 133 allocations when warm, versus about 3.21 ms, 986 KB, and 60,014 allocations for repeated heap Top-N: about 123x faster, 46x less heap, and 451x fewer allocations. Each distinct field/direction order is independently admitted after repeated reads and costs one 4-byte row ordinal per cached row within the same 4 MB bound. |
| Numeric segment Top-N pruning, 20,000 rows and `LIMIT 50` | About 62.7 us, 28 KB, and 423 allocations with existing numeric segment metadata, versus about 1.25 ms, 186 KB, and 20,167 allocations without it: about 20x faster, 6.6x less heap, and 47.7x fewer allocations. It creates no additional metadata and falls back when a numeric sidecar is unavailable. |
| Opt-in adaptive typed-table numeric segments, selective 4,096-row Top-N | About 44.7 us, 27.8 KB, and 244 allocations, versus fixed 256-row segments at 56.9 us, 29.4 KB, and 436 allocations: about 1.27x faster, 1.06x less allocated heap, and 1.8x fewer allocations. It retains roughly 2.3 KB more min/max sidecar metadata for two numeric columns, so defaults remain unchanged. |
| Opt-in JSON `LOWER(name)` equality index, 10,000 rows and 100 matching rows | About 65.9 us, 110 KB, and 739 allocations, versus a 12.22 ms, 7.89 MB, and 135,246-allocation scan: about 185x faster, 72x less allocated heap, and 183x fewer allocations. It retains lowercase postings and the existing JSON source snapshot only after `CreateSQLJSONLowerIndex`; a missing index, admission denial, non-string source value, or non-string predicate literal retains the ordinary scan and SQL behavior. |

## Deliberately Deferred

### MVCC Typed-Table Versions

Keeping historical versions for every typed-table write would make old-reader
queries lock-free, but it also retains overwritten rows until all readers
advance. This project currently has stable snapshot locking and immutable
query-result publication. Without a workload showing reader lock contention,
MVCC would add write and memory cost with no measured query win. It remains a
separate proposal, not a default behavior.

### Immutable Parts And Background Merge

ClickHouse-style immutable parts would help append-heavy persistent analytic
tables, but require a storage format, atomic manifest publication, merge
budgeting, recovery checks, backup integration, and compaction benchmarking.
`hatrie_cache` currently targets compact mutable cache and typed-table paths;
adding a second storage engine now would increase recovery and backup
complexity. Reconsider this only with an append-heavy persisted workload and
a benchmark that includes write throughput, scan throughput, peak memory,
crash recovery, backup, and restore.

### More Generic SQL Rewrites

The generic SQL executor intentionally does not infer that an arbitrary query
equals a typed aggregate. Automatic rewrites risk semantic mismatches around
filters, NULLs, aliases, ordering, and source versions. Existing exact
columnar order projections and explicit typed aggregate arrangements provide
the measurable benefit without guessing.

## Re-evaluation Gate

Do not implement a deferred item until a deterministic benchmark and
regression suite demonstrate exact output equivalence, no unacceptable write
or heap regression, and recovery behavior across compaction and restart.
