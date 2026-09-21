# Engine Idea Catalog

This is the implementation backlog for ideas observed in ClickHouse,
Materialize, and Tarantool that are not currently represented as complete
public capabilities in `hatrie_cache`. The adopted matrix in
`ADOPTED_QUERY_ENGINE_IDEAS.md` remains the source of truth for features that
are already implemented.

Each candidate must follow the repository workflow before adoption: add a
focused red correctness test, capture a deterministic baseline, implement the
smallest compatible surface, rerun correctness and race checks, measure CPU,
heap, allocations, and relevant I/O, and roll back when the tradeoff is not
worthwhile. Large distributed features are cataloged here even when they need
an explicit design phase before coding. Partitioning remains preferred over
automatic sharding for multi-datacenter deployments.

Reference material: [ClickHouse documentation](https://clickhouse.com/docs/en/),
[Materialize concepts](https://materialize.com/docs/fundamentals/concepts/),
[Materialize arrangements](https://materialize.com/docs/get-started/arrangements/),
[Tarantool indexes](https://www.tarantool.io/en/doc/latest/book/box/box_space/index/),
and [Tarantool Vinyl](https://www.tarantool.io/en/doc/latest/platform/engines/vinyl/).

## ClickHouse candidates

Candidates remain listed for traceability. Implemented ideas are recorded in
[ADOPTED_QUERY_ENGINE_IDEAS.md](ADOPTED_QUERY_ENGINE_IDEAS.md).

| ID | Candidate not yet complete here | Current gap and likely value | Initial risk |
| --- | --- | --- | --- |
| CH-001 | Explicit `PREWHERE` stage | Implemented for stream-capable single-source reads; specialized physical plans still use a combined predicate. | Low |
| CH-002 | Sparse primary-key mark pruning | Ordered JSON indexes now prune literal ranges with binary-search bounds; physical part/mark granularity is still deferred. | Medium |
| CH-003 | Partition-key pruning | Partially adopted: SQL now forwards validated literal equality, `IN`, and range predicates to opt-in partition resolvers; concrete physical partition metadata remains backend-specific. | Medium |
| CH-004 | `FINAL` read semantics | Partially adopted: explicit SQL `FINAL` reconciles caller-defined replacing or collapsing source rows; automatic schema-bound metadata and persistent merge integration remain open. | High |
| CH-005 | Lightweight delete bitmap | Partially adopted: opt-in typed-table patch parts now persist their compact bitmap through bounded CRC-protected state snapshots; immutable stored-part manifests and cross-process part integration remain open. | High |
| CH-006 | Mutation dependency queue | Partially adopted: importable `SQLMutationDependencyQueue` now persists dependency-aware task transitions in a CRC-checked binary WAL with crash-tail recovery and explicit compaction; automatic SQL `ALTER`/`DELETE` wiring and cross-process leases remain open. | High |
| CH-007 | Row TTL | Adopted as an opt-in `TypedTable` processing-time or event-time policy with an indexed explicit purge, a shared bounded background scheduler, and CRC-protected processing-time deadline snapshots. Column-level TTL, automatic schema integration, and retry policy remain caller-owned. | Medium |
| CH-008 | Column TTL | Partially adopted: `TypedTableColumn.TTL` independently masks expired cells in row/columnar reads, `Stats`, and `Histogram`; explicit and scheduler-driven purge physically clears values and emits updates, with bounded CRC-protected processing-time deadline snapshots. | Medium |
| CH-009 | TTL rollup | Partially adopted: importable `TypedTableTTLRollup` aggregates expired row images, and `TypedTableTTLScheduler.RegisterWithRollup` wires it into an explicit opt-in maintenance pass; persistence and automatic historical replay remain caller-owned. | High |
| CH-010 | Materialized/default columns | Partially adopted: `TypedTableColumn.GeneratedMode` adds explicit materialized/default write semantics, cached dependency ordering, and schema validation; SQL expression parsing, DDL wiring, and persistence remain caller-owned. | Medium |
| CH-011 | General projection DDL | Partially adopted: `SQLSession` supports session-local `CREATE`, `DROP`, and explicit `REFRESH PROJECTION` backed by source-version-guarded materialized snapshots; durable table-bound metadata, automatic source notifications, and cross-node coordination remain open. | High |
| CH-012 | Projection advisor | Partially adopted: opt-in `CostBasedRecommendations` compares bounded observed average latency with caller-supplied projection-hit, build, and refresh costs using saturating arithmetic; automatic workload forecasting, planner wiring, and persistent advisor state remain open. | Medium |
| CH-013 | Query condition cache | Adopted in the existing opt-in `SQLQueryConditionCache`: versioned columnar predicate matches use bounded LRU state, skip unversioned or drifting sources, and preserve exact SQL rows. | Medium |
| CH-014 | Uncompressed hot-data cache | No cache that stores decoded hot ranges while preserving compressed storage. | Medium |
| CH-014b | Mutation dependency graph with resumable progress | Implemented as an in-memory reverse dependency index plus deterministic ready min-heap; empty polls avoid graph-wide scans and allocations, while snapshot/WAL formats remain compatible. See [CH014B_MUTATION_DEPENDENCY_READY.md](CH014B_MUTATION_DEPENDENCY_READY.md). | Medium |
| CH-015 | Filesystem cache admission | Adopted as opt-in `RemotePartCacheOptions.MinAccesses`; zero preserves eager admission, while positive thresholds suppress one-hit retention with bounded candidate metadata. See [CH015_FILESYSTEM_CACHE_ADMISSION.md](CH015_FILESYSTEM_CACHE_ADMISSION.md). | Medium |
| CH-016 | Asynchronous insert queue | Writes cannot be acknowledged before bounded background batching. | Medium |
| CH-017 | Async-insert deduplication | No idempotency token ledger for safely retrying queued inserts. | Medium |
| CH-018 | Insert quorum | No write acknowledgement policy requiring a configured replica quorum. | High |
| CH-019 | Replicated-part checks | No per-part checksums and replica consistency repair workflow. | High |
| CH-020 | Zero-copy part sharing | No remote part registration that avoids copying immutable storage between replicas. | High |
| CH-021 | Object-storage tiering | No hot/local and cold/object-storage tier with transparent reads. | High |
| CH-022 | Incremental part backup | Adopted as content-addressed object storage with manifest-level changed-object accounting and an optional durable `BackupManifestCatalog` for incremental chain planning; retention execution remains caller-managed. | Medium |
| CH-023 | Selective partition restore | Partially adopted for opt-in snapshot restores: a validated partition/prefix subset can be restored atomically; checkpoint-only markers and a safe single-key replay tail are handled, while complex replay commands and persistent-store subset restores remain rejected. | Medium |
| CH-024 | Detach/attach parts | No operator API to quarantine immutable parts and attach verified replacements. | Medium |
| CH-025 | Merge-pool prioritization | Compaction lacks a user-visible priority scheduler balancing freshness and space. | Medium |
| CH-026 | Merge selector policies | No configurable size-tiered or time-aware merge selector. | Medium |
| CH-027 | Background-task observability | Implemented generic `CompactionScheduler.Ages()` timestamps and caller-supplied age helpers alongside existing queue/outcome stats; task-specific bytes and TTL metrics remain provider-owned. | Low |
| CH-028 | Query `max_threads` setting | Implemented bounded per-query `SETTINGS max_threads = N` over the existing worker path; session-wide defaults remain caller-owned. | Low |
| CH-029 | User/key quotas | Implemented opt-in bounded sharded sliding-window quotas keyed by caller identity for query count, result bytes, and elapsed execution time; nil remains disabled. | Medium |
| CH-030 | Query complexity limits | Partially adopted: SQL options already bound rows, joins, results, bytes, skew, and now total `GROUP BY` keys; per-expression and CPU quotas remain future work. | Medium |
| CH-031 | Persistent query log | Implemented as an opt-in privacy-safe newline-delimited JSON log attached to `SQLQueryManager`; terminal status survives process restart without retaining SQL text, sources, parameters, or cancellation reasons. | Low |
| CH-032 | Query profiler samples | Adopted as an opt-in bounded `hatSql` API keyed by query ID, with configurable sampling, CPU/blocking/row/byte fields, deterministic snapshots, and no default executor overhead. | Done |
| CH-033 | Distributed query fan-out | One query cannot plan and merge reads from multiple independent nodes. | High |
| CH-034 | Parallel replicas | No coordinated replica reads that divide ranges and merge ordered results. | High |
| CH-035 | Remote shard pruning | No shard-level predicate routing before distributed execution. | High |
| CH-036 | `ASOF JOIN` | Partially adopted: constrained `ASOF [LEFT] JOIN` with one equality and one temporal inequality, keyed right-side buckets, and binary search. | High |
| CH-037 | `ARRAY JOIN` | No row-expanding array join operator with SQL NULL semantics. | Medium |
| CH-038 | Aggregate combinators | `COUNT_IF`/`COUNTIF`, numeric `*_IF`, `ARGMAX_IF`/`ARGMIN_IF`, and opt-in `COUNT_STATE`/`SUM_STATE`/`AVG_STATE`/`MIN_STATE`/`MAX_STATE` with matching `*_MERGE` are adopted; `OrNull` remains absent. | Medium |
| CH-039 | Approximate distinct/quantile sketches | Partially adopted: global `APPROX_COUNT_DISTINCT` and `APPROX_PERCENTILE` queries now feed bounded sketches while streaming; grouped, top-k, and other complex shapes remain on the materialized evaluator. | Medium |
| CH-040 | `argMax`/`argMin` aggregates | Implemented for ordinary, grouped, filtered, and window aggregates, with a constant-state stream fast path for eligible global scans. `ARGMAX_STATE`/`ARGMIN_STATE` and matching merge functions add explicit transferable partial winners. | Low |
| CH-041 | `GROUPING SETS`/`ROLLUP`/`CUBE` | Partially adopted: existing multi-level expansion now supports `GROUPING(expr)` identifiers folded per branch; native one-pass grouping and multi-argument `GROUPING_ID` remain open. | High |
| CH-042 | Sampling key execution | `SAMPLE` is not a storage-aware deterministic sampling stage. | Medium |
| CH-043 | Gap filling/interpolation | Implemented bounded ordered time-bucket `WITH FILL` plus opt-in `PREVIOUS`, `NEXT`, and numeric `LINEAR` interpolation; unconfigured queries retain the original fill path. | Medium |
| CH-044 | JSON dynamic subcolumns | Implemented as an opt-in bounded `HatTrie` adapter over automatic typed scalar JSON subcolumns; cold paths fall back, writes invalidate by source generation, and warm path-only queries avoid retaining full decoded rows. | High |
| CH-045 | Array/map subcolumn pruning | No read planner that loads only referenced nested subcolumns. | Medium |
| CH-046 | Native wire protocol framing | No ClickHouse-style typed block protocol with explicit column framing and progress. | High |
| CH-047 | Parallel format parsing | Partially adopted: `hatSql.ParseNDJSONParallel` decodes independent NDJSON line ranges concurrently while preserving input order and deterministic lowest-line errors; CSV block parsing remains open because quoted newlines require a safe framing pass. | Medium |
| CH-048 | SIMD/generic predicate coverage | Partially adopted: plain binary string comparisons now use a direct zero-allocation columnar kernel for all six ordering/equality operators, while packed/dictionary, non-binary collation, and broader predicate shapes remain on existing paths. | Medium |
| CH-049 | Refreshable external dictionaries | Adopted as an opt-in `hatSql` registry with immutable atomic snapshots, manual/background refresh, bounded staleness, and `DICT_GET`/`DICT_GET_OR_DEFAULT`/`DICT_HAS`. | Done |
| CH-050 | Named settings collections and inheritable validated profiles | Adopted as an imported `hatSql` registry with bounded immutable snapshots, revision compare-and-swap updates, optional parent inheritance with cycle/depth/effective-size checks, caller-supplied setting validation, isolated full-profile reads, and allocation-free single-value lookup. | Done |

## Materialize candidates

| ID | Candidate not yet complete here | Current gap and likely value | Initial risk |
| --- | --- | --- | --- |
| MZ-001 | Durable persist shards | No durable collection shard abstraction that can hydrate without rereading the source. | High |
| MZ-002 | Snapshot frontier gating | Source reads do not expose a public readiness frontier that blocks until a consistent snapshot. | Medium |
| MZ-003 | Hydration progress and resume | Historical state rebuild has no public byte/record progress or resumable checkpoint. | Medium |
| MZ-004 | Logical compaction controls | Partially adopted: opt-in `hatPipeline.FrontierCompactionScheduler` and `FrontierRetentionRegistry.WaitUntilSafe` gate caller-owned history removal behind source frontiers and read holds; per-collection policy, durable jobs, and automatic prioritization remain open. | High |
| MZ-005 | Since/upper frontier introspection | Partially adopted: `TypedTableChangeReadHold` exposes bounded `Since`/`Upper` frontiers for multi-call changefeed readers and protects compaction; frontiers for every maintained object remain deferred. | Medium |
| MZ-006 | Antichain timestamps | No partially ordered timestamp frontier for concurrent partitions. | High |
| MZ-007 | Stale-read rejection | Adopted: opt-in `SQLSourceFrontierResolver` validation rejects unavailable, unready, or stale non-local sources before reads, including joins, CTEs, and set-operation branches. | Medium |
| MZ-008 | `AS OF` query execution | Adopted: opt-in `SQLQueryOptions.AsOfFrontier` binds normal, streamed, offset-page, and keyset-page reads to an immutable `SQLFrontierSnapshotProvider` view; historical reads bypass the live result cache and bind cursors to the frontier. | High |
| MZ-009 | Temporal validity filters | Partially adopted: `VALID_AT(at, valid_from, valid_to)` provides first-class half-open validity semantics and uses native scalar execution when eligible; validity indexes and frontier-aware pruning remain open. | High |
| MZ-010 | `TAIL`/`SUBSCRIBE` changefeed | Partially adopted: opt-in `CommandJournal.Subscribe` replays a contiguous bounded command tail and follows live journal records with bounded backpressure; importable `QuerySubscriptions.SubscribeSQL` and `SubscribeDifferentialSQL` now derive static `CACHE(...)` dependencies while preserving bounded snapshot/differential delivery. SQL statement grammar and signed wire envelopes remain open. | High |
| MZ-011 | Sink connector API | Partially adopted: importable `CommandJournalSink` and bounded `StartCommandJournalSink` runner support ordered batches, cancellation, and optional sequence checkpoints; concrete external connectors and exactly-once transactions remain open. | High |
| MZ-012 | Exactly-once sink checkpoints | Partially adopted: `CommandJournalExactlyOnceSink` lets a sink atomically commit output with its journal watermark and reload that watermark on restart; the guarantee depends on the external sink transaction. | High |
| MZ-013 | Source connector checkpoints | Partially adopted: importable `CommandJournalSourceCheckpointCoordinator` persists binary source offsets with the latest fully applied journal sequence through `WithPersistenceBarrier`; durable store transactions and connector-specific polling remain store/connector-owned. | High |
| MZ-014 | Upsert-source consolidation | Partially adopted: importable `hatDataStructure.UpsertBatch[T]` keeps one final value or tombstone per key in stable source order with reusable capacity; connector flush policy and automatic SQL integration remain caller-owned. | Medium |
| MZ-015 | CDC envelope normalization | Partially adopted: importable `hatSql.NormalizeCDCEnvelope` validates dynamic external envelopes, canonicalizes common Debezium/Materialize/Tarantool operation aliases, preserves source sequence and rows, and provides JSON decoding; source offsets and connector-specific row conversion remain caller-owned. | Medium |
| MZ-016 | Source schema evolution | No additive/drop-column compatibility plan for live source versions. | High |
| MZ-017 | Parallel source hydration | Partially adopted: partitioned snapshot and Pebble hydration already dispatch deterministic partition work to workers; `ConfigureSnapshotRestoreWorkers` adds a bounded operator cap, while non-partitioned source-range splitting remains deferred. | Medium |
| MZ-018 | Compute/storage separation | Partially adopted: `SQLQueryManager` can run managed queries on an opt-in bounded `hatPipeline` compute pool with independent worker and queue limits; `0` preserves caller-goroutine execution, while durable storage and distributed compute remain caller-owned. | High |
| MZ-019 | Per-cluster resource isolation | Partially adopted: `NamespaceQueryGovernor` supports opt-in named per-namespace `hatPipeline` compute pools with independent worker and queue admission; the default remains caller-goroutine execution, and process-wide RSS enforcement or dynamic resizing remain deferred. | Medium |
| MZ-020 | Worker scaling coordination | Implemented as an opt-in `hatPipeline.ResizableScheduler`: online target changes preserve queued work and let running tasks finish; the existing fixed scheduler remains unchanged. | High |
| MZ-021 | Replica hot handoff | No ready replica transfer that avoids a cold query-state rebuild. | High |
| MZ-022 | Index hydration readiness | Partially adopted: `HatTrie.WaitSQLJSONIndexReady` is a synchronous, context-aware readiness barrier that coalesces and runs one cooperative rebuild until the configured index is current; background scheduling and incremental frontier reporting remain available separately. | Medium |
| MZ-023 | `IN CLUSTER` index placement | Indexes cannot be assigned to an isolated compute pool. | Medium |
| MZ-024 | Automatic arrangement key selection | Partially adopted: bounded deterministic scoring marks the best existing arrangement for `EXPLAIN` workloads across `WHERE`, `GROUP BY`, `ORDER BY`, and joins; automatic arrangement creation and execution rewrites remain caller-owned. | Medium |
| MZ-025 | Arrangement sharing by logical key | Existing sharing is definition-based; no canonical equivalence for semantically equal plans. | Medium |
| MZ-026 | Dictionary arrangement compression | Repeated arrangement strings are not transparently dictionary encoded. | Medium |
| MZ-027 | Arrangement memory telemetry | Implemented as an explicit read-only stats API reporting per-arrangement distinct values, bounded retained-byte estimates, checkpoints, source sequence, changelog compaction watermark, and dictionary group-order rebuild counts. | Low |
| MZ-028 | Adaptive arrangement compaction | No feedback loop that changes compaction cadence from memory and update rates. | Medium |
| MZ-029 | Spillable arrangements | Adopted as an opt-in bounded local payload spill tier with binary records, CRC validation, disk limits, exact cold reads, and explicit compaction; fully disk-resident arrangement indexes and reopen/restore remain future work. | High |
| MZ-030 | Differential join delta maintenance | Adopted as a reusable exact inner-join maintainer with signed multiplicities, atomic batches, deterministic snapshots, and a replacement fast path; SQL planner wiring and outer/temporal joins remain future work. | High |
| MZ-031 | Skew-aware join exchange | Adopted as an imported deterministic routing policy with bounded heavy-key tracking, build-side broadcast, probe-side spreading, and generation-fenced rebalancing; automatic planner/executor integration remains future work. | High |
| MZ-032 | Late-data reclocking | Adopted as an importable bounded remap sidecar with monotone source/processing frontiers, late-event assignment, compaction, and CRC-validated snapshots; source inference and automatic SQL/connector wiring remain future work. | High |
| MZ-033 | Timestamp oracle | Adopted in existing importable `hatReplication`: process-local atomic `TimestampOracle` plus transport-neutral `GlobalTimestampOracle` with monotone observed time, contiguous range grants, node-epoch restart fencing, idempotent sequence retries, term fencing, local leases, and validated snapshots; consensus, replication, and durability remain caller-owned. | High |
| MZ-034 | Generic negative-diff operators | Partially adopted as an importable `hatSql` differential API for signed filter/map/flat-map/union/join/difference/intersection and grouped aggregate changes; automatic planner wiring and multiset coverage across every SQL operator remain open. | High |
| MZ-035 | Multiset preservation everywhere | Duplicate multiplicities are not retained consistently across all operators. | High |
| MZ-036 | Recursive fixpoint scheduler | Recursive dataflow lacks a general iterative frontier scheduler. | High |
| MZ-037 | Incremental Top-K maintenance | Partially adopted as importable exact weighted differential maintenance: `hatSql.IncrementalTopK` keeps keyed rows ordered in a treap, applies signed multiplicities atomically, and emits only Top-K transitions; automatic SQL planner/operator wiring remains. | High |
| MZ-038 | Incremental order arrangement | Partially adopted: opt-in typed-table columnar caches can admit a bounded single-field `[]uint32` order projection after repeated `ORDER BY ... LIMIT` requests; composite orders, range cursors, and fully incremental arbitrary updates remain. | High |
| MZ-039 | General incremental distinct | Partially adopted as importable stateful signed distinct maintenance: `hatSql.IncrementalDistinct` retains multiplicities across batches and emits only set-membership transitions; automatic SQL planner/operator wiring remains. | Medium |
| MZ-040 | Incremental percentile | Partially adopted as an importable exact weighted order-statistics operator: `hatSql.IncrementalPercentile` maintains keyed multiplicities in an ordered treap and answers nearest-rank percentile reads without a full sort; automatic SQL planner wiring, distributed arrangements, and approximate mergeable sketches remain. | High |
| MZ-041 | View freshness SLA | Implemented as optional per-task max-staleness status for managed materialized-view and rollup refreshes; existing scheduling remains unchanged by default. | Medium |
| MZ-042 | Dependency invalidation graph | Implemented as an automatic reverse source-to-view index for `MaterializedViews.RefreshChanged`; affected candidates are deduplicated and still sorted/published atomically. | Medium |
| MZ-043 | Transactional DDL dependencies | DDL cannot atomically create/alter a source, view, index, and dependent sink plan. | High |
| MZ-044 | Costed dataflow explanation | Adopted as explicit `EXPLAIN COST` operator CPU and memory heuristics; steps without cardinality estimates omit cost fields, and normal execution remains unchanged. See [MZ044_COSTED_EXPLAIN.md](MZ044_COSTED_EXPLAIN.md). | Medium |
| MZ-045 | Workload plan equivalence | Partially implemented through `SQLCompiledQueryCache`: equivalent whitespace and keyword-casing token streams share one bounded compiled plan while literal values and schema versions remain distinct; query-fingerprint-to-arrangement reuse is still open. | Low |
| MZ-046 | Frontier-aware cancellation | Adopted as opt-in `hatPipeline.NewFrontierCancellation`, which derives a context canceled at a named `FrontierRegistry` lower frontier with immediate already-reached handling and explicit causes; query and pipeline integration remains caller-owned. | Low |
| MZ-047 | Session compute routing | Adopted as opt-in `SQLQueryManagerOptions.ComputeClusters` plus `QueryOptions.ComputeCluster`, with bounded named pools, deterministic discovery, synchronous unknown-name validation, and draining `Close`; distributed scheduling and automatic workload classification remain caller-owned. | Medium |
| MZ-048 | Connector secret rotation | Adopted as an opt-in `ConnectorCredentialRotator` and serialized `ConnectorRegistry.RotateCredentials` path with version and bounded-size validation; `hatAuth.ResourceRegistry` handoff remains explicit and connector-owned atomic publication/retry policy remain caller-owned. See [MZ048_CONNECTOR_SECRET_ROTATION.md](MZ048_CONNECTOR_SECRET_ROTATION.md). | Medium |
| MZ-049 | Exactly-once snapshot export | Implemented as opt-in `CommandJournal.WriteSnapshotWithResumableExport`: an immutable manifest-bound source, fsynced prefix checkpoints, tail truncation, and atomic publication prevent duplicate bytes after interruption. See [MZ049_SNAPSHOT_EXPORT.md](MZ049_SNAPSHOT_EXPORT.md) and [BENCHMARK.md](BENCHMARK.md#mz-049-exactly-once-snapshot-export). | High |
| MZ-050 | Timeline branching and replay | Implemented as `hatCache.CommandJournal.BranchAt`: an isolated in-memory branch can apply successful what-if commands and replay the branch into an empty trie without mutating the source journal or trie. See [MZ050_TIMELINE_BRANCH.md](MZ050_TIMELINE_BRANCH.md) and [BENCHMARK.md](BENCHMARK.md#mz-050-timeline-branching-and-replay). | High |

## Tarantool candidates

| ID | Candidate not yet complete here | Current gap and likely value | Initial risk |
| --- | --- | --- | --- |
| TT-001 | Automatic vshard bucket rebalancing | Partition plans exist, but no automatic data movement and ownership convergence. | High |
| TT-002 | Bucket ownership consensus | Partition ownership is not committed through a consensus-backed metadata log. | High |
| TT-003 | Router failover route cache | Adopted as opt-in `hatReplication.FailoverRouteCache` with generation-fenced replacements, stable key routing, atomic lock-free lookups, bounded failure cooldowns, and explicit success/invalidation reporting; dialing, retry, and health inference remain caller-owned. See [TT003_FAILOVER_ROUTE_CACHE.md](TT003_FAILOVER_ROUTE_CACHE.md). | Medium |
| TT-004 | Synchronous batch replication quorum | Single writes and batches do not provide rollback-free cluster-wide quorum commit. | High |
| TT-005 | Raft configuration state | No consensus-backed configuration and membership state machine. | High |
| TT-006 | Hot-standby WAL catch-up | No read-only standby that continuously replays and can be promoted without restore. | High |
| TT-007 | Snapshot-plus-WAL join | A joining node cannot hydrate from a snapshot and then apply a bounded WAL delta automatically. | High |
| TT-008 | Incremental snapshot chains | Snapshots are not content-addressed deltas with a verified parent chain. | Medium |
| TT-009 | Backup manifest checksums | Implemented: backup manifests record per-file sizes and SHA-256 hashes, and restore/doctor verification checks them before publication. | Low |
| TT-010 | Selective space backup | Partially adopted: snapshot bundles accept explicit logical key-prefix scope and record it in the manifest; Pebble checkpoint and incremental repository backups remain full-store only. | Medium |
| TT-011 | Point-in-time incremental restore | Implemented as an opt-in snapshot-bundle restore through an exact committed journal sequence; default `MaxJournalSequence=0` preserves complete restore behavior. | Medium |
| TT-012 | Per-space storage engine choice | A logical data structure cannot independently choose memory and LSM persistence policies. | High |
| TT-013 | Vinyl range tuple cache | Storage has no range-aware cache that retains only hot key intervals. | High |
| TT-014 | Vinyl compaction throttling | Compaction does not expose adaptive disk/latency throttles and backpressure. | Medium |
| TT-015 | Vinyl read/write thread tuning | Read and compaction workers lack independent bounded runtime configuration. | Medium |
| TT-016 | Disk-space reserve admission | Implemented as an opt-in filesystem free-space reserve for LevelDB/Pebble full, key, dirty, and generation saves; default remains disabled. | Low |
| TT-017 | Run-level Bloom filters | Implemented as an opt-in native LevelDB/Pebble Bloom prefilter for new persistent runs and replication-outbox tables; default remains `0` because the measured warm workload adds 11.7%-14.4% storage with no general CPU win. | Medium |
| TT-018 | Page-index residency policy | Adopted as opt-in `hatDataStructure.PageIndexResidency[K,V]` with byte/entry-bounded LRU eviction, zero-allocation hits, and admission/eviction counters; storage integration remains caller-owned. | Medium |
| TT-019 | Covering secondary indexes | Secondary postings cannot retain selected payload fields to avoid primary lookups. | Medium |
| TT-020 | Generic multi-part TREE ranges | Implemented as the allocation-free `OrderedIndex.Range` API, which binary-searches inclusive composite bounds and iterates only the bounded subslice; callers can express a partial-key prefix with the smallest and largest suffix values. | Medium |
| TT-021 | RTREE spatial index | Adopted as the opt-in immutable `hatDataStructure.PackedRTree[T]` bounding-box API; mutable updates, SQL planner wiring, and persistence remain open. | High |
| TT-022 | BITSET index | Adopted by the existing generic `hatDataStructure.BitmapIndex[K]` RoaringBitmap-backed API (`Add`, `Remove`, `Contains`, `Rows`, `Visit`, `Union`, and `Intersect`); SQL planner wiring remains caller-owned. | Medium |
| TT-023 | HASH equality index | Implemented as a zero-allocation raw-string fast path for homogeneous ordinary SQL JSON field equality indexes; mixed-type values retain the existing typed key encoding. | Low |
| TT-024 | Full-text phrase/position index | `CONTAINS_PREFIX` now uses an opt-in sorted token-key sidecar; token positions and phrase search remain absent. | High |
| TT-025 | Online uniqueness validation | Unique index creation has no staged validation before atomic publication. | Medium |
| TT-026 | Versioned tuple format | Implemented as opt-in `hatDataStructure.VersionedTuple` schema validation and bounded HTV1 envelope; existing unversioned tuple caches remain compatible. | High |
| TT-027 | Generated columns | No write-maintained expression columns with dependency validation. | Medium |
| TT-028 | Upsert conflict handlers | Public mutation commands lack declarative merge-on-conflict callbacks or policies. | Medium |
| TT-029 | General before/after replace triggers | Trigger scope is not a complete pre/post mutation lifecycle for every command. | Medium |
| TT-030 | Transactional DDL | Schema/index changes cannot be atomically grouped with data mutations. | High |
| TT-031 | Stream API | Implemented: public gRPC `CommandStream` and `CommandBatchStream` provide request streaming with ordered responses for the legacy path. | Medium |
| TT-032 | IProto-style multiplexing | Partially adopted: opt-in nonzero `request_id` correlation and bounded `CommandStreamWorkers` allow independent commands to overlap; zero-ID streams and the default worker count preserve legacy ordering. | High |
| TT-033 | Fiber scheduler quotas | Cooperative tasks lack per-tenant CPU and queue budgets. | Medium |
| TT-034 | Cooperative task cancellation | Background fibers do not share a standard cancellation token and drain state. | Low |
| TT-035 | Per-request deadlines | Command APIs lack a consistent deadline propagated through storage and replication. | Low |
| TT-036 | Storage `box.stat` equivalent | Partially adopted: `CompactionScheduler.Stats()` adds low-overhead maintenance queue and outcome counters; storage-engine operation, page, cache, and WAL counters remain provider-owned. | Low |
| TT-037 | Audit log | Implemented as opt-in `hatAudit` redaction applied before recent retention, JSONL persistence, and sink export; existing lossless behavior remains the default. See [TT037_AUDIT_REDACTION.md](TT037_AUDIT_REDACTION.md) and [BENCHMARK.md](BENCHMARK.md#tt-037-audit-metadata-redaction). | Medium |
| TT-038 | Roles and grants for commands | Command access is not modeled as a per-operation role/privilege matrix. | Medium |
| TT-039 | Transparent credential rotation | Authentication credentials cannot rotate with overlapping validity and no restart. | Medium |
| TT-040 | Space changefeed | Implemented as an opt-in `CommandJournal.SubscribeSpace` stream that replays and follows committed records for one exact logical space key while preserving global journal sequences. | High |
| TT-041 | Crash-safe index resume | Implemented as an opt-in durable SQL JSON index rebuild checkpoint store with atomic file persistence, restart recovery, and retry-safe completion. | Medium |
| TT-042 | Restore resume checkpoints | Implemented as opt-in deterministic staging reuse for bundle and incremental repository restore, with checksum revalidation, stale-entry pruning, and CLI/API controls. | Medium |
| TT-043 | Maintenance read-only mode | Implemented as default-off public command gating for HTTP and gRPC; reads, snapshots, backups, and internal replay paths remain available. | Low |
| TT-044 | Schema migration dry run | Implemented as importable `hatSchema.Preview`, which validates a migration on an independent schema copy without publication. | Low |
| TT-045 | Tuple-level compression | Implemented as an opt-in bounded `HTC1` frame codec with adaptive ZSTD admission, raw fallback, reusable scratch storage, CRC32 validation, and pre-decode size limits. | Medium |
| TT-046 | Slab/memory accounting | Implemented read-only per-structure native/backing accounting and monitoring; bounded eviction/admission telemetry remains a separate follow-up. | Medium |
| TT-047 | Expiration wheel | Partially adopted: the opt-in cleaner now sleeps until the indexed min-heap deadline and wakes for newly earlier deadlines, including local partitions; a hierarchical wheel remains deferred because exact heap deadlines preserve bounded live entries and simpler recovery semantics. | Medium |
| TT-048 | Queue/priority space primitive | Partially adopted as an importable priority visibility queue with explicit durable checkpoints. | Medium |
| TT-049 | Pessimistic row locks | Partially adopted as an importable bounded `SQLRowLockManager` with context-aware leases, capacity limits, idle-key reclamation, and deterministic per-key serialization; SQL grammar, transaction wiring, and distributed fencing remain caller-owned. | High |
| TT-050 | SQL planner statistics | Partially adopted: explicit source-versioned `ANALYZE` statistics now feed what-if planning, while `TypedTable.Stats()` and numeric `TypedTable.Histogram()` reuse invalidation-aware exact snapshots; opt-in `SaveSQLPlannerStatistics`/`LoadSQLPlannerStatistics` HPS1 snapshots validate source SHA-256 digests, while a full cost model remains. | Medium |

## Selection order

The first implementation candidates beyond the adopted rows should be additive
and measurable: persistent backup manifests, background task metrics, bounded
TTL expiration, and the remaining cost-model portion of source-versioned SQL
planner statistics. Consensus,
distributed fan-out, durable dataflow state, and automatic repartitioning need
separate designs because they affect backup, recovery, and correctness across
nodes.
