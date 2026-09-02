# Query Engine Ideas: Adoption Status

This matrix records the ClickHouse, Materialize, and Tarantool ideas assessed
for `hatrie_cache`. An idea is adopted only when it preserves exact query or
recovery behavior and either improves a measured workload or supplies an
explicitly opt-in operational control.

| Source | Idea | Status | Evidence |
|---|---|---|---|
| Materialize | Coordinated progress frontier | Adopted | `SQLProjectionRetentionFrontier` commits journal retention only after all configured runners succeed. [PROJECTION_FRONTIERS.md](PROJECTION_FRONTIERS.md) |
| Materialize | Shared arrangements | Adopted | `TypedTableAggregateArrangements` shares exact aggregate state among identical definitions, and `TypedTableJoinArrangements` shares exact incremental equi-joins. [TYPED_TABLE_ARRANGEMENTS.md](TYPED_TABLE_ARRANGEMENTS.md), [TYPED_TABLES.md](TYPED_TABLES.md) |
| Materialize | Differential extrema | Adopted | `TypedTableAggregateDefinition` now accepts opt-in numeric `MinField` and `MaxField`. Each group retains counted distinct values and only rescans that group when its final current extreme is removed, so inserts, updates, and deletes remain exact. |
| Materialize | Differential distinct aggregate | Adopted | `TypedTableAggregateDefinition.DistinctField` retains per-group multiplicities for one opt-in typed scalar field, emitting exact `count_distinct` across inserts, updates, deletes, and replay. |
| ClickHouse | LowCardinality typed strings | Adopted | `TypedTableColumn.DictionaryEncoded` is opt-in and retains live string values once with compact row codes; ordinary string columns retain their existing layout. |
| Tarantool | Partial equality index | Adopted | `CreateSQLJSONPartialIndex` retains postings for one lookup field only where a second field equals a fixed configured literal. The existing composite resolver selects it only for the matching equality conjunction. |
| ClickHouse/Tarantool | Reusable indexed dimension postings | Adopted | `BorrowedIndexedSourceResolver` lets an immutable ordinary HatTrie JSON equality posting list serve an eligible SQL join without cloning its candidate rows for every probe. Existing `IndexedSourceResolver` implementations remain the fallback, and hot-key plans retain the existing hash join. |
| ClickHouse | Projection for repeated ordering | Adopted | Immutable cached ordinal projections serve repeated single-column and multi-column `ORDER BY` Top-N queries, including mixed directions, after repeated reads. |
| ClickHouse | Dynamic Top-N data skipping | Adopted | Cached numeric segment min/max metadata skips only segments that cannot beat the bounded Top-N threshold; equal-boundary segments remain scanned to preserve stable ties. |
| ClickHouse | Ordered primary-key version parts | Adopted at the temporal-table boundary | `TemporalTable` keeps each key's versions time-ordered. Chronological writes append, late writes are placed after equal timestamps, and `AS OF` uses binary search while preserving copy isolation. |
| ClickHouse | Granular skipping indexes | Already present | Numeric min/max, dictionary membership, string equality Bloom, and n-gram Bloom sidecars prune only impossible segments. |
| ClickHouse | Narrow expression index | Adopted | `CreateSQLJSONLowerIndex` is an explicit `LOWER(field) = literal` equality index. It uses the existing generation-aware JSON snapshot lifecycle and falls back to the SQL scan for source values that are not strings. |
| Materialize | Literal semi-join index union | Adopted | Direct-field and `LOWER(direct field)` `IN (literal, ...)` predicates union existing equality postings after duplicate-literal removal. `NOT IN`, subqueries, other expressions, non-binary collations, and unavailable indexes retain the normal evaluator. |
| Tarantool | Controlled cooperative maintenance | Adopted | `ManagedRefreshScheduler` now has opt-in count and duration cycle budgets. [REFRESH_SCHEDULER.md](REFRESH_SCHEDULER.md) |
| Tarantool | Consistent read snapshot | Adopted | `SnapshotLocker` gives resolvers a stable query lifetime, while opt-in `TypedTableMVCCOptions` adds immutable historical typed-table snapshots without changing the default mutable path. [TYPED_TABLES.md](TYPED_TABLES.md) |
| ClickHouse/Tarantool | Immutable persistent parts / LSM storage | Adopted for persistent cache storage | `PebbleStore` uses Pebble's immutable SSTable and background-compaction path, with generation checkpoints, crash recovery, backup, and restore verification. |
| ClickHouse | Lightweight delete patch parts | Adopted for opt-in typed tables | `TypedTablePatchOptions` records delete tombstones without moving typed rows, skips them in row and columnar SQL reads, and provides threshold-triggered or explicit compaction. Updates remain on the existing path and the feature is disabled by default. [TYPED_TABLES.md](TYPED_TABLES.md) |
| ClickHouse | Dynamic JSON path skip metadata | Adopted for opt-in JSON sources | `CreateSQLJSONPathSkipIndex` stores bounded per-segment Bloom metadata for normalized nested paths. It prunes equality candidates while the SQL executor rechecks the original predicate, so false positives cannot change results. |
| Materialize | Bounded arrangement hydration | Adopted as an explicit control | `TypedTableAggregateArrangement.Hydrate` and `TypedTableJoinArrangement.Hydrate` replay retained source changes in bounded batches, with `Freshness` reports and explicit compacted-history errors. No background worker or default memory behavior changes. |

## Measured Results

| Feature | Result |
|---|---|
| Shared typed aggregate arrangement, two consumers over 10,000 changes | 2.02x faster, 1.98x less heap, and 1.99x fewer allocations than two independent aggregates. |
| Shared typed equi-join arrangement, one updated row with 10,000 rows per side and 64 string join keys | About 16.4 us, 1.0 KB, and 5 allocations, versus a 1.53 s, 634 MB, 3,175,006-allocation full rebuild: about 93,000x faster, 634,000x less allocated heap, and 635,000x fewer allocations. Factorized structural pairs retain source keys without cloning row values or serializing pair identities; same-kind string keys avoid a redundant prefix. It is explicit because retained state grows with matching-pair cardinality. |
| Typed equi-join coalesced same-key changes, two updates and 1,024 matches | About 2.56 us, 344 B, and 6 allocations, versus separate updates at 197 us, 192 KB, and 32 allocations: about 77x faster, 557x less allocated heap, and 5.3x fewer allocations. It preserves ordered validation and checkpoint-prefix behavior. |
| Incremental typed-table MIN/MAX, one non-extreme update in a 10,000-row group | Median about 1.61 us, 1.37 KB, and 14 allocations, versus a full min/max rescan at 2.98 ms, 4.64 MB, and 49,745 allocations: about 1,850x faster, 3,390x less allocated heap, and 3,550x fewer allocations. The per-group value multiset is retained only when `MinField` or `MaxField` is configured; removing a final current extreme rescans that group's distinct values. |
| Incremental typed-table COUNT DISTINCT, one non-final-value update in a 10,000-row group with 1,000 values | Median about 1.48 us, 1.35 KB, and 14 allocations, versus a full distinct-set rescan at 3.28 ms, 4.66 MB, and 47,446 allocations: about 2,200x faster, 3,400x less allocated heap, and 3,390x fewer allocations. The per-group value multiplicity map is retained only when `DistinctField` is configured; NULL is excluded. |
| Opt-in typed-table dictionary strings, 10,000 rows and 8 values | Median about 183 us and 188 KB, versus plain string headers at 182 us and 713 KB: effectively equal construction time with about 3.8x less retained allocation. It adds eight setup allocations for dictionary bookkeeping and is disabled by default. |
| Opt-in partial JSON equality index, 10,000 rows with `active = true` at 10% selectivity | Median refresh about 217 us, 25.9 KB, and 1,013 allocations, versus a general composite index at 1.10 ms, 568 KB, and 20,031 allocations: about 5.1x faster, 22x less allocated heap, and 19.8x fewer allocations. It is used only when the query includes the configured fixed equality. |
| Borrowed equality postings, 100 fact probes into a 10,000-row indexed dimension | Median about 546 us, 531 KB, and 2,589 allocations, versus copied postings at 588 us, 565 KB, and 2,889 allocations: about 1.08x faster, 1.06x less allocated heap, and 1.12x fewer allocations. A hot 10,000-probe one-row dimension was rejected: forcing indexed probes was about 1.28x slower, 1.14x more heap, and 1.20x more allocations than the retained hash join. |
| Temporal version ordering, 10,000 chronological writes and a latest `AS OF` read | Chronological `Upsert` median about 3.10 ms, 4.91 MB, and 29,763 allocations, versus about 860 ms, 5.95 MB, and 59,761 allocations with a full history sort: about 277x faster, 1.21x less allocated heap, and 2.01x fewer allocations. Latest `AS OF` is about 223 ns versus 4.2 us, about 18.8x faster, with the same 336 B and 2 allocations for result isolation. Out-of-order histories still insert in order; this rare path shifts the suffix but avoids an unnecessary complete sort. |
| Projection retention frontier commit | 207 ns median, 0 B/op, 0 allocs/op; background-only and opt-in. |
| Refresh scheduler count budget | No measurable overhead over the default scheduler in the no-op refresh benchmark. |
| Refresh scheduler duration budget | About 568 ns, 352 B, and 5 allocations per opt-in no-op cycle due to the cooperative timeout context. |
| Composite columnar sorted projection, 20,000 rows and `LIMIT 50` | About 15.9 us, 21 KB, and 131 allocations when warm, versus about 2.30 ms, 986 KB, and 60,014 allocations for repeated heap Top-N: about 145x faster, 46x less heap, and 458x fewer allocations. The admitted index costs one 4-byte row ordinal per cached composite order, within the existing 4 MB layout-cache bound. |
| Mixed-direction composite columnar sorted projection, 20,000 rows and `LIMIT 50` | About 26.1 us, 21 KB, and 133 allocations when warm, versus about 3.21 ms, 986 KB, and 60,014 allocations for repeated heap Top-N: about 123x faster, 46x less heap, and 451x fewer allocations. Each distinct field/direction order is independently admitted after repeated reads and costs one 4-byte row ordinal per cached row within the same 4 MB bound. |
| Numeric segment Top-N pruning, 20,000 rows and `LIMIT 50` | About 62.7 us, 28 KB, and 423 allocations with existing numeric segment metadata, versus about 1.25 ms, 186 KB, and 20,167 allocations without it: about 20x faster, 6.6x less heap, and 47.7x fewer allocations. It creates no additional metadata and falls back when a numeric sidecar is unavailable. |
| Opt-in adaptive typed-table numeric segments, selective 4,096-row Top-N | About 44.7 us, 27.8 KB, and 244 allocations, versus fixed 256-row segments at 56.9 us, 29.4 KB, and 436 allocations: about 1.27x faster, 1.06x less allocated heap, and 1.8x fewer allocations. It retains roughly 2.3 KB more min/max sidecar metadata for two numeric columns, so defaults remain unchanged. |
| Opt-in JSON `LOWER(name)` equality index, 10,000 rows and 100 matching rows | About 65.9 us, 110 KB, and 739 allocations, versus a 12.22 ms, 7.89 MB, and 135,246-allocation scan: about 185x faster, 72x less allocated heap, and 183x fewer allocations. It retains lowercase postings and the existing JSON source snapshot only after `CreateSQLJSONLowerIndex`; a missing index, admission denial, non-string source value, or non-string predicate literal retains the ordinary scan and SQL behavior. |
| JSON equality-index literal `IN`, 10,000 rows and 10 distinct literals | About 21.5 us, 18.2 KB, and 121 allocations, versus a 12.0 ms, 8.21 MB, and 90,066-allocation scan: about 557x faster, 452x less allocated heap, and 744x fewer allocations. Duplicate literals are removed before probing; full predicate evaluation preserves SQL null behavior. |
| JSON `LOWER(name)` index literal `IN`, 10,000 rows and 3 normalized literals | About 235 us, 330 KB, and 2,334 allocations, versus a 14.0 ms, 8.13 MB, and 145,645-allocation scan: about 60x faster, 25x less allocated heap, and 62x fewer allocations. Duplicate literals are removed before probing; non-string literals retain the normal scan path. |

| Bounded typed arrangement hydration, one tail update after 10,000 changes | Median about 1.35 us, 400 B, and 7 allocations, versus about 1.30 ms, 560.6 KB, and 20,009 allocations for a full aggregate rebuild: about 960x lower latency, 1,402x less allocated heap, and 2,858x fewer allocations. The feature replays only retained changes, is caller-scheduled, and returns `ErrTypedTableChangesCompacted` when the source no longer retains the required prefix. |

| Opt-in typed-table MVCC snapshots, 1,000-row writes and current-snapshot reads | Median repeated writes measured 1.12x slower with 18% more heap and two additional allocations; current-snapshot row materialization was 1.10x slower with 0.1% more heap and four additional allocations. Historical reads are the benefit, so MVCC remains disabled by default. |
| Opt-in typed-table lightweight delete patches, 10,000 rows | Delete followed by reinsert was 1.16x faster with 5.9% less heap and the same four allocations; 50%-deleted `Rows` was 1.03x faster with the same allocation profile. Explicit compaction cost 35.4 us for 1,000 rows and 0.52 ms for 10,000 rows. Tombstoned backing capacity remains until the normal storage lifecycle releases it, so this optimizes row movement rather than immediate heap reclamation. |
| Opt-in JSON path skip metadata, 20,000 rows and 100 nested matches | Warmed nested equality execution was about 4.77x faster, used about 2.91x less allocated heap, and made about 6.91x fewer allocations than the full JSON scan. The index retains 512 bits per 256-row segment in this fixture and remains disabled unless configured. |

## Deliberately Deferred

### Additional Typed-Table Immutable Parts And Background Merge

ClickHouse-style immutable parts would help append-heavy persistent analytic
typed tables, but the persistent cache already gets immutable SSTables and
background compaction from `PebbleStore`. Adding a second in-memory typed-table
storage engine would require a separate row format, atomic manifest
publication, merge budgeting, recovery checks, backup integration, and
compaction benchmarking. Reconsider this only with an append-heavy persisted
typed-table workload and a benchmark that includes write throughput, scan
throughput, peak memory, crash recovery, backup, and restore.

### More Generic SQL Rewrites

The generic SQL executor intentionally does not infer that an arbitrary query
equals a typed aggregate. Automatic rewrites risk semantic mismatches around
filters, NULLs, aliases, ordering, and source versions. Existing exact
columnar order projections and explicit typed aggregate arrangements provide
the measurable benefit without guessing.

## Re-evaluation Gate

Do not implement another deferred item until a deterministic benchmark and
regression suite demonstrate exact output equivalence, no unacceptable write
or heap regression, and recovery behavior across compaction and restart.
### Generic PREWHERE / late materialization

Status: adopted automatically for simple single-source `CACHE` queries when
the resolver implements `StreamSQLSource`. The predicate is evaluated while
rows stream in, and only rows inside `OFFSET/LIMIT` receive retained projected
maps. The source is still drained for resource-limit and late-error
compatibility. Queries requiring joins, grouping, ordering, typed sources,
subqueries, or custom functions retain the established executor.

Measured with `make benchmark-sql-prewhere`: median 2.39x lower latency, 24.6x
lower heap, and 2.00x fewer allocations on a 20,000-row selective projection
benchmark. See [BENCHMARK.md](BENCHMARK.md#generic-prewhere--late-materialization)
for raw samples and workload details.
