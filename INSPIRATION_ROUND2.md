# Product Inspiration Round 2

This is a second, concrete backlog of capabilities inspired by ClickHouse,
Materialize, and Tarantool. The first broad inventory is in
[INSPIRATION.md](INSPIRATION.md); its current audit marks the previously
identified items adopted or deliberately deferred. The items below are
additional capabilities that were not represented as a named, verified
capability in the current code audit.

`[ ]` means pending. An item becomes `[x]` only after a focused test is written
first, the implementation is measured, broader verification passes, and the
change is published. A measured regression or unacceptable tradeoff becomes
`[-]` with the raw result recorded in [BENCHMARK.md](BENCHMARK.md). Automatic
sharding remains out of scope by default; regional partitioning and explicit
operator control remain the preferred deployment model.

## Research Sources

- [ClickHouse feature journey](https://clickhouse.com/clickhouse/feature-journey)
- [ClickHouse query cache and concurrency guidance](https://clickhouse.com/resources/engineering/high-concurrency-sizing-user-analytics)
- [ClickHouse lightweight updates](https://clickhouse.com/blog/updates-in-clickhouse-2-sql-style-updates)
- [Materialize SUBSCRIBE](https://materialize.com/docs/sql/subscribe/)
- [Materialize durable subscriptions](https://materialize.com/docs/transform-data/patterns/durable-subscriptions/)
- [Materialize optimization and indexes](https://materialize.com/docs/transform-data/optimization/)
- [Materialize concepts](https://materialize.com/docs/fundamentals/concepts/)
- [Tarantool platform documentation](https://www.tarantool.io/en/doc/latest/singlepage/)
- [Tarantool synchronous replication](https://www.tarantool.io/en/doc/latest/book/replication/repl_sync/)
- [Tarantool automated leader election](https://www.tarantool.io/en/doc/latest/book/replication/repl_leader_elect/)
- [Tarantool indexes](https://www.tarantool.io/en/doc/latest/book/box/box_space/index/)

## ClickHouse: 50 Additional Ideas

- [-] C201 Adaptive asynchronous-insert flush timeout based on arrival rate. Rejected after the end-to-end 64-concurrent-command benchmark measured `432-478 us/op`, `67,0xx B/op`, and `334 allocs/op` versus the existing static worker at `158-164 us/op`, `66,5xx B/op`, and `331 allocs/op`; the adaptive path was about 2.8-3.0x slower. See [BENCHMARK.md](BENCHMARK.md#c201-rejected-adaptive-asynchronous-batch-flush).
- [x] C202 Per-shard asynchronous-insert buffer affinity to reduce cross-shard coordination. Implemented as explicit `hatPipeline.PartitionedAsyncBatcher`; see [C202_PARTITIONED_ASYNC_BATCHER.md](C202_PARTITIONED_ASYNC_BATCHER.md) and [BENCHMARK.md](BENCHMARK.md#c202-partition-affine-asynchronous-batching).
- [x] C203 Explicit `wait_for_async_insert` durability modes with visible acknowledgment semantics. Implemented for opt-in HTTP async commands; omitted/`0` preserves `202` admission and `1` waits for durable-and-applied completion. See [CHU03_ASYNC_INSERT_ACK_MODES.md](CHU03_ASYNC_INSERT_ACK_MODES.md).
- [x] C204 Idempotency-token propagation across asynchronous inserts and dependent materialized views. Implemented through incremental projections and refreshed materialized-view status; see [C204_PROJECTION_IDEMPOTENCY.md](C204_PROJECTION_IDEMPOTENCY.md) and [BENCHMARK.md](BENCHMARK.md#c204-projection-idempotency-metadata).
- [x] C205 Query-cache controls scoped to individual subqueries; see [C205_SUBQUERY_RESULT_CACHE.md](C205_SUBQUERY_RESULT_CACHE.md) and [BENCHMARK.md](BENCHMARK.md#c205-subquery-result-cache).
- [x] C206 Query-cache eligibility checks that reject nondeterministic expressions; verified in [C206_QUERY_CACHE_ELIGIBILITY.md](C206_QUERY_CACHE_ELIGIBILITY.md).
- [x] C207 Query-condition cache with data-generation invalidation for repeated filters; verified in [C207_QUERY_CONDITION_CACHE.md](C207_QUERY_CONDITION_CACHE.md).
- [x] C208 Query-cache hit, miss, bypass, and eviction metrics via `ResultCache`; see [C208_RESULT_CACHE_METRICS.md](C208_RESULT_CACHE_METRICS.md).
- [x] C209 Automatic basic column statistics for row count, null count, min, and max via `TypedTable.Stats()`; see [C209_TYPED_TABLE_STATS.md](C209_TYPED_TABLE_STATS.md).
- [x] C210 Compact histograms for cardinality and selectivity estimates via `TypedTable.Histogram()`; see [C210_TYPED_TABLE_HISTOGRAM.md](C210_TYPED_TABLE_HISTOGRAM.md).
- [x] C211 Statistics-driven join-order selection with deterministic fallback via `SourceCardinalityResolver`; see [C211_STATISTICS_JOIN_ORDER.md](C211_STATISTICS_JOIN_ORDER.md).
- [x] C212 Precomputed typed join probe keys for hot hash-table paths; numeric and boolean probes avoid canonical-string allocation, while strings retain the native map path; architecture-specific software prefetch remains intentionally deferred. See [C212_PRECOMPUTED_JOIN_HASH.md](C212_PRECOMPUTED_JOIN_HASH.md).
- [x] C213 Cached compiled SQL plan handles with bounded entry and estimated-byte memory; opt-in through `SQLQueryOptions.CompiledCache`. Machine-code JIT remains intentionally out of scope. See [C213_COMPILED_PLAN_CACHE.md](C213_COMPILED_PLAN_CACHE.md).
- [x] C214 Resource-bounded WebAssembly UDF execution; see [C214_BOUNDED_WASM_UDF.md](C214_BOUNDED_WASM_UDF.md).
- [x] C215 External dictionary reloads with TTL and last-known-good retention; see [SQL_EXTERNAL_DICTIONARIES.md](SQL_EXTERNAL_DICTIONARIES.md).
- [x] C216 Dictionary layout selection based on key cardinality and lookup shape; see [C216_COLUMNAR_DICTIONARY_SHAPES.md](C216_COLUMNAR_DICTIONARY_SHAPES.md).
- [x] C217 Time-series `WITH FILL` gap generation over ordered results; see [WITH_FILL.md](WITH_FILL.md).
- [x] C218 Interpolation policies for filled time-series values; see [C218_WITH_FILL_INTERPOLATION.md](C218_WITH_FILL_INTERPOLATION.md).
- [x] C219 Per-group `LIMIT BY` execution with bounded memory; see [SQL_LIMIT_BY.md](SQL_LIMIT_BY.md) and [BENCHMARK.md#c219-limit-by](BENCHMARK.md#c219-limit-by).
- [x] C220 Post-window `QUALIFY` filtering over projected window rows; selected window aliases are evaluated before `DISTINCT`, `ORDER BY`, and `LIMIT`; see [C220_QUALIFY.md](C220_QUALIFY.md).
- [x] C221 `WITH TIES` limit semantics for deterministic boundary results; see [LIMIT_WITH_TIES.md](LIMIT_WITH_TIES.md).
- [x] C222 Approximate top-K aggregation with mergeable bounded state; importable `hatCache.TopK.Merge`, HAG1 aggregate-state round trips, and `HatTrie.MergeTopK` provide bounded partition-state union without changing existing command defaults. SQL planner integration remains open.
- [x] C223 Mergeable approximate distinct and quantile aggregate states; importable HLL, Count-Min, and TDigest states support validated partition merges and compact transfer. SQL aggregate `State`/`Merge` syntax remains open.
- [x] C223a Mergeable HyperLogLog partial state; `HyperLogLog.Merge` combines same-precision partition states with per-register maxima, supports zero-value receiver adoption, and rejects invalid or mismatched states without mutation. SQL `State`/`Merge` syntax remains open under C223.
- [x] C223b Compact mergeable TDigest aggregate state; fixed-width centroid payloads use HAG1 framing, validate compression/count/order/finite values, and support atomic merge-from-wire without changing existing defaults. An existing large-input centroid-bound mismatch remains a separate compaction task.
- [x] C223b Mergeable Count-Min Sketch partial state; exported `CountMinSketch` snapshots merge same-shape counter matrices with saturating addition, and `HatTrie.MergeCountMinSketch` imports an owned state without changing existing command defaults. SQL `State`/`Merge` syntax remains open under C223.
- [x] C223c Versioned partial aggregate envelopes; importable HLL and Count-Min state APIs now use bounded, checksummed HAG1 frames with compact raw-register/counter payloads, explicit kind/version metadata, and strict decoder validation. SQL `State`/`Merge` integration remains open under C223.
- [x] C224 `argMax` and `argMin` aggregate states with deterministic tie handling; see [SQL_ARG_EXTREME.md](SQL_ARG_EXTREME.md).
- [x] C225 Incremental window-frame state for repeated ordered windows; the materialized executor now maintains growing `SUM`/`AVG`/`MIN`/`MAX` state for the exact unbounded-preceding `ROWS` shape and falls back for complex frames. See [C225_INCREMENTAL_WINDOW.md](C225_INCREMENTAL_WINDOW.md) and [BENCHMARK.md#c225-incremental-ordered-window-frames](BENCHMARK.md#c225-incremental-ordered-window-frames).
- [x] C226 Grace-hash join spilling with bounded disk runs; the existing 64-partition path bounds the right-side build chunk, output spill, and temporary-file lifecycle for eligible equality joins. See [C226_GRACE_HASH_JOIN.md](C226_GRACE_HASH_JOIN.md) and [BENCHMARK.md#c226-grace-hash-join-spilling](BENCHMARK.md#c226-grace-hash-join-spilling).
- [x] C227 External aggregation spilling with merge-time memory limits; `SQLQueryOptions.MaxGroupMergeBytes` bounds the decoded spill-reader frontier, stays off by default, and cleans temporary files on rejection. See [C227_GROUP_MERGE_BUDGET.md](C227_GROUP_MERGE_BUDGET.md) and [BENCHMARK.md#c227-external-group-merge-memory-budget](BENCHMARK.md#c227-external-group-merge-memory-budget).
- [x] C228 External sort spilling with stable run ordering; the existing spill record ordinal is preserved through run files and merge passes, and equal-key stability is covered by [C228_EXTERNAL_SORT_STABILITY.md](C228_EXTERNAL_SORT_STABILITY.md) and [BENCHMARK.md#c228-stable-external-sort-runs](BENCHMARK.md#c228-stable-external-sort-runs).
- [x] C229 Explicit join overflow policy for auto, reject, and bounded spill; truncation remains intentionally unsupported to preserve SQL correctness. See [C229_JOIN_OVERFLOW_POLICY.md](C229_JOIN_OVERFLOW_POLICY.md) and [BENCHMARK.md#c229-join-overflow-policy](BENCHMARK.md#c229-join-overflow-policy).
- [x] C230 Memory-overcommit wait queues before query cancellation; the opt-in SQL memory queue shares bounded operator reservations across queries, waits before cancellation, removes canceled waiters, and preserves the default path. See [C230_MEMORY_OVERCOMMIT.md](C230_MEMORY_OVERCOMMIT.md) and [BENCHMARK.md#c230-memory-overcommit-wait-queues](BENCHMARK.md#c230-memory-overcommit-wait-queues).
- [x] C231 Workload groups with per-class concurrency and memory budgets. SQL queries can opt into `SQLClusterAdmission` through `SQLQueryOptions`; see [C231_SQL_WORKLOAD_GROUPS.md](C231_SQL_WORKLOAD_GROUPS.md) and [BENCHMARK.md#c231-sql-workload-groups](BENCHMARK.md#c231-sql-workload-groups).
- [x] C232 Query-complexity limits for rows, joins, and intermediate results.
- [x] C233 Per-query CPU-time budgets with cooperative cancellation. See [C233_CPU_TIME_BUDGET.md](C233_CPU_TIME_BUDGET.md) and [BENCHMARK.md](BENCHMARK.md#c233-per-query-cpu-time-budget).
- [x] C234 Query profiler records for stage time, bytes, and allocations. See [C234_QUERY_STAGE_PROFILER.md](C234_QUERY_STAGE_PROFILER.md) and [BENCHMARK.md](BENCHMARK.md#c234-query-stage-profiler).
- [x] C235 Read/write task profiler aggregation by table part and column; bounded `hatSql.SQLQueryProfiler.RecordPartColumn` with deterministic snapshots and explicit dropped-observation counters. See [CH235_PART_COLUMN_PROFILER.md](CH235_PART_COLUMN_PROFILER.md) and [BENCHMARK.md#c235-part-column-query-profiling](BENCHMARK.md#c235-part-column-query-profiling).
- [x] C236 Explain output for data-skipping-index decisions and rejected marks. Already implemented under CH-U49 for direct equality probes over selected `CACHE` JSON-path skip indexes; `EXPLAIN ANALYZE` exposes candidate/skipped rows and segments without changing query results. See [CHU49_SKIP_INDEX_EXPLAIN.md](CHU49_SKIP_INDEX_EXPLAIN.md) and [BENCHMARK.md#ch-u49-skip-index-explain-diagnostics](BENCHMARK.md#ch-u49-skip-index-explain-diagnostics).
- [x] C237 Explain output for projection selection and estimated I/O cost. See [CH237_PROJECTION_EXPLAIN.md](CH237_PROJECTION_EXPLAIN.md) and [BENCHMARK.md#c237-projection-selection-explain-output](BENCHMARK.md#c237-projection-selection-explain-output).
- [x] C238 Mutation queue progress with rows remaining and elapsed estimates. Already implemented under C036: `CommandJournal` and `ReplicationOutboxStore` expose durable replay progress and ETA through `ReplayWithProgress`. See [REPLAY_PROGRESS.md](REPLAY_PROGRESS.md) and [BENCHMARK.md](BENCHMARK.md#c036-durable-mutation-queue).
- [x] C239 Part-merge backlog, amplification, and age metrics. `hatStorage.SnapshotCompactionMetrics` combines scheduler backlog/age and bounded cumulative compaction byte diagnostics without allocations. See [C239_COMPACTION_METRICS.md](C239_COMPACTION_METRICS.md) and [BENCHMARK.md#c239-compaction-metrics-snapshot](BENCHMARK.md#c239-compaction-metrics-snapshot).
- [x] C240 Read-only backup database attachment for querying backup parts in place. `OpenBackupReadOnlyAttachment` verifies and stages snapshot bundles, Pebble checkpoints, and incremental repositories into a detached read-only SQL resolver; `Close` removes staging. See [C240_READ_ONLY_BACKUP_ATTACHMENT.md](C240_READ_ONLY_BACKUP_ATTACHMENT.md) and [BENCHMARK.md#c240-read-only-backup-attachment](BENCHMARK.md#c240-read-only-backup-attachment).
- [x] C241 Incremental backup chunk deduplication across snapshots. Large incremental repository files default to 1 MiB content-addressed chunks, with verified chunk restore, retention accounting, pooled buffers, and `RepositoryChunkSize: -1` legacy fallback. See [C241_INCREMENTAL_BACKUP_CHUNK_DEDUP.md](C241_INCREMENTAL_BACKUP_CHUNK_DEDUP.md) and [BENCHMARK.md#c241-incremental-backup-chunk-deduplication](BENCHMARK.md#c241-incremental-backup-chunk-deduplication).
- [x] C242 Parallel restore of independent parts with bounded concurrency. See [C242_PARALLEL_RESTORE.md](C242_PARALLEL_RESTORE.md) and [BENCHMARK.md](BENCHMARK.md#c242-bounded-parallel-restore).
- [x] C243 Remote-part read-through caching with immutable checksum keys is implemented by `hatStorage.RemotePartCache`. See [C243_REMOTE_PART_CACHE.md](C243_REMOTE_PART_CACHE.md), [REMOTE_PART_CACHE.md](REMOTE_PART_CACHE.md), and [BENCHMARK.md](BENCHMARK.md#c243-remote-part-read-through-cache).
- [x] C244 Local cache reuse validated by part and column checksums is implemented by the opt-in `hatMerkle.PartManifest`. See [C244_LOCAL_CACHE_REUSE.md](C244_LOCAL_CACHE_REUSE.md) and [BENCHMARK.md](BENCHMARK.md#c244-local-cache-reuse-validation).
- [x] C245 Vertical TTL deletion that reads only the deletion mask and key columns is implemented as an opt-in `hatDataStructure.PersistentDeleteBitmap` operation with a reusable zero-allocation buffer form. See [C245_VERTICAL_TTL_DELETE.md](C245_VERTICAL_TTL_DELETE.md) and [BENCHMARK.md](BENCHMARK.md#c245-vertical-ttl-deletion).
- [x] C246 TTL-driven recompression policies separate from row deletion are implemented as an opt-in `hatDataStructure.TTLRecompressionPolicy` and `TupleCompressor.RecompressIfDue`. See [C246_TTL_RECOMPRESSION.md](C246_TTL_RECOMPRESSION.md) and [BENCHMARK.md](BENCHMARK.md#c246-ttl-driven-recompression).
- [x] C247 Adaptive `uint64` delta and Gorilla `float64` compression with raw fallback are implemented as opt-in `hatDataStructure` codecs; ALP-style floating-point compression remains open. See [C247_UINT64_DELTA_CODEC.md](C247_UINT64_DELTA_CODEC.md).
- [x] C248 Variant/JSON subcolumn projection that reads only referenced paths is adopted through the existing typed scalar and opt-in dynamic JSON subcolumns. See [C248_VARIANT_JSON_SUBCOLUMNS.md](C248_VARIANT_JSON_SUBCOLUMNS.md) and [BENCHMARK.md](BENCHMARK.md#c248-variantjson-subcolumn-projection).
- [x] C249 Kafka-style offset inspection without committing consumer position is implemented as a bounded read-only `hatReplication.SpaceChangefeed.Inspect` API. See [C249_OFFSET_INSPECTION.md](C249_OFFSET_INSPECTION.md) and [BENCHMARK.md](BENCHMARK.md#c249-kafka-style-offset-inspection).
- [x] C250 Retry-safe insert identities shared across asynchronous ingestion stages are already implemented as the opt-in bounded `hatPipeline.AsyncInsertDeduplicator`, with optional durable CRC-protected storage. See [C250_ASYNC_INSERT_IDENTITIES.md](C250_ASYNC_INSERT_IDENTITIES.md) and [BENCHMARK.md](BENCHMARK.md#c250-retry-safe-async-insert-identities).

## Materialize: 50 Additional Ideas

- [x] M201 Changefeed progress messages that certify a timestamp frontier. `hatReplication.ChangefeedFrontier` publishes allocation-free monotonic sequence progress with idempotent equal advances and regression rejection; see [CHANGEFEED_PROGRESS.md](CHANGEFEED_PROGRESS.md).
- [x] M202 Durable subscription resume from a persisted `AS OF` frontier. `hatReplication.ChangefeedCheckpoint` provides a strict source-bound binary checkpoint that advances only from valid monotonic progress; see [CHANGEFEED_CHECKPOINT.md](CHANGEFEED_CHECKPOINT.md).
- [x] M203 Snapshot-free subscription mode for consumers that already have state; see [M203_SNAPSHOT_FREE_SUBSCRIPTIONS.md](M203_SNAPSHOT_FREE_SUBSCRIPTIONS.md).
- [x] M204 Bounded subscriptions with an exclusive `UP TO` sequence; see [M204_BOUNDED_SUBSCRIPTIONS.md](M204_BOUNDED_SUBSCRIPTIONS.md).
- [x] M205 Deterministic within-timestamp ordering for changefeed batches. Opt-in `QuerySubscriptionDefinition.DeterministicOrder` sorts differential initial, update, progress-safe, and reset batch phases by canonical row key without changing default behavior; see [M205_DETERMINISTIC_SUBSCRIPTION_ORDER.md](M205_DETERMINISTIC_SUBSCRIPTION_ORDER.md).
- [x] M206 Upsert envelopes that expose a stable key and current row image. `hatSql.UpsertChangefeed` emits detached keyed current-image envelopes and tombstones with subscription frontier metadata; see [M206_UPSERT_ENVELOPES.md](M206_UPSERT_ENVELOPES.md) and [BENCHMARK.md#mz-016-keyed-upsert-envelopes](BENCHMARK.md#mz-016-keyed-upsert-envelopes).
- [x] M207 Debezium envelopes with before/after images and operation type. `hatSql.DebeziumChangefeed` requires declared unique key columns, emits snapshot/create/update/delete payloads, preserves subscription frontier metadata, and rejects ambiguous multiplicity; the optimized adapter measured about 16x lower latency, 18x lower allocated bytes, and 10x fewer allocations than its full-state-copy baseline; see [M207_DEBEZIUM_CHANGEFEED.md](M207_DEBEZIUM_CHANGEFEED.md).
- [x] M208 Differential multiplicity folding for insert/delete update streams. See [M208_DIFFERENTIAL_MULTIPLICITY_FOLDING.md](M208_DIFFERENTIAL_MULTIPLICITY_FOLDING.md).
- [x] M209 Monotone logical timestamp frontiers for read and stream APIs. See [M209_MONOTONE_LOGICAL_TIMESTAMP.md](M209_MONOTONE_LOGICAL_TIMESTAMP.md).
- [x] M210 Historical `AS OF` reads against retained logical state. See [M210_RETAINED_SQL_SNAPSHOTS.md](M210_RETAINED_SQL_SNAPSHOTS.md) and [BENCHMARK.md#m210-retained-sql-snapshots](BENCHMARK.md#m210-retained-sql-snapshots).
- [x] M211 Explicit rejection of reads before `since` or at/after `upper` frontiers. See [M211_SQL_FRONTIER_BOUNDS.md](M211_SQL_FRONTIER_BOUNDS.md) and [BENCHMARK.md#m211-sql-frontier-bounds](BENCHMARK.md#m211-sql-frontier-bounds).
- [x] M212 Logical compaction that advances retained history without rewriting live state. See [M212_LOGICAL_COMPACTION.md](M212_LOGICAL_COMPACTION.md).
- [x] M213 Consolidation of equal updates before forwarding to downstream consumers is already covered by query row grouping and M208 adapter folding; the additional enqueue fold was measured and rejected. See [M213_EQUAL_UPDATE_CONSOLIDATION.md](M213_EQUAL_UPDATE_CONSOLIDATION.md).
- [x] M214 Arrangement reuse across indexes and compatible query plans; already covered by compiled arrangement workloads, canonical plan caching, and typed-table arrangement registries. See [M214_ARRANGEMENT_REUSE_AUDIT.md](M214_ARRANGEMENT_REUSE_AUDIT.md).
- [x] M215 Delta-join maintenance for high-churn join inputs; already implemented by `hatSql.IncrementalJoin` with signed differential updates and equality-bucket maintenance. See [M215_DELTA_JOIN_AUDIT.md](M215_DELTA_JOIN_AUDIT.md).
- [x] M216 Incremental top-K arrangements with bounded replacement state; already implemented by `hatSql.IncrementalTopK`, with selected/replacement output bounded by K and full exact source indexing retained. See [M216_INCREMENTAL_TOP_K_AUDIT.md](M216_INCREMENTAL_TOP_K_AUDIT.md).
- [x] M217 Indexes that store complete maintained view results for point lookups; added opt-in maintained point lookup indexes to `MaterializedViews` with atomic refresh integration. See [M217_MATERIALIZED_POINT_LOOKUP.md](M217_MATERIALIZED_POINT_LOOKUP.md).
- [x] M218 Planner selection of point lookup versus arrangement scan; added the opt-in `MaterializedViewPointLookupResolver` and benchmarked point-vs-scan execution. See [M218_MATERIALIZED_POINT_LOOKUP_PLANNER.md](M218_MATERIALIZED_POINT_LOOKUP_PLANNER.md).
- [x] M219 Background index creation with observable build frontier; added opt-in asynchronous maintained point-lookup builds with atomic publication, stale-snapshot rejection, cancellation, and measurable progress. See [M219_BACKGROUND_POINT_LOOKUP_BUILD.md](M219_BACKGROUND_POINT_LOOKUP_BUILD.md).
- [x] M220 Safe index removal after dependent readers drain; added point-lookup reader leases, observable retirement state, stale-reader isolation, and a synchronous drop wrapper that waits for drain. See [M220_POINT_LOOKUP_RETIREMENT.md](M220_POINT_LOOKUP_RETIREMENT.md).
- [x] M221 Isolated compute clusters with independent resource budgets; already covered by opt-in `hatSql.SQLClusterAdmission` with independent named-cluster and serving/maintenance budgets. See [M221_CLUSTER_COMPUTE_ISOLATION_AUDIT.md](M221_CLUSTER_COMPUTE_ISOLATION_AUDIT.md).
- [x] M222 Replicated compute workers for highly available maintained indexes; added opt-in `MaterializedViewComputeReplicaSet` refresh fanout, health fencing, and healthy-replica point-read failover. See [M222_REPLICATED_COMPUTE_WORKERS.md](M222_REPLICATED_COMPUTE_WORKERS.md).
- [x] M223 Hydration state machines that distinguish cold, hydrating, and ready views. See [M223_MATERIALIZED_VIEW_HYDRATION.md](M223_MATERIALIZED_VIEW_HYDRATION.md).
- [x] M224 Hydration progress and estimated remaining work metrics. See [M224_MATERIALIZED_HYDRATION_PROGRESS.md](M224_MATERIALIZED_HYDRATION_PROGRESS.md).
- [x] M225 Persisted shard leases that prevent duplicate state ownership. See [M225_PERSISTED_SHARD_LEASES.md](M225_PERSISTED_SHARD_LEASES.md).
- [x] M226 Durable consensus metadata for state shard and frontier ownership. See [M226_DURABLE_CONSENSUS_METADATA.md](M226_DURABLE_CONSENSUS_METADATA.md).
- [x] M227 Source snapshot offsets coupled atomically to the first live frontier. See [M227_ATOMIC_SNAPSHOT_FRONTIER.md](M227_ATOMIC_SNAPSHOT_FRONTIER.md).
- [x] M228 Exactly-once source restart from a committed source offset. See [M228_EXACTLY_ONCE_SOURCE_RESTART.md](M228_EXACTLY_ONCE_SOURCE_RESTART.md).
- [x] M229 Source schema evolution with additive field compatibility checks. See [M229_SOURCE_SCHEMA_EVOLUTION.md](M229_SOURCE_SCHEMA_EVOLUTION.md).
- [x] M230 Source backpressure based on downstream frontier lag. See [M230_SOURCE_BACKPRESSURE.md](M230_SOURCE_BACKPRESSURE.md).
- [x] M231 Exactly-once upsert sinks with durable output identities. See [M231_EXACTLY_ONCE_UPSERT_SINK.md](M231_EXACTLY_ONCE_UPSERT_SINK.md).
- [x] M232 Sink progress checkpoints coupled to emitted frontier messages. See [M232_SINK_PROGRESS_ENVELOPES.md](M232_SINK_PROGRESS_ENVELOPES.md).
- [x] M233 Sink retry and deduplication for disconnected output connections. See [M233_SINK_RETRY_OUTBOX.md](M233_SINK_RETRY_OUTBOX.md).
- [x] M234 Sink backpressure and bounded pending-output memory. See [M234_SINK_BACKPRESSURE.md](M234_SINK_BACKPRESSURE.md).
- [x] M235 Dependency graph invalidation for affected indexes and views only. `MaterializedViews` maintains a reverse dependency index and `RefreshChanged` visits only affected views.
- [x] M236 On-demand refresh of only invalidated maintained objects. `RefreshChanged` skips maintained objects whose dependencies were not changed.
- [x] M237 Lazy hydration triggered by the first reader with cancellation support. See [M237_LAZY_HYDRATION.md](M237_LAZY_HYDRATION.md).
- [x] M238 Explain output for filter pushdown and arrangement reuse. `EXPLAIN` now emits a `FILTER_PUSHDOWN` notice, while `Arrangements[].Reused` exposes arrangement reuse. See [M238_EXPLAIN_PUSHDOWN.md](M238_EXPLAIN_PUSHDOWN.md).
- [x] M239 Explain output for logical timestamp and frontier requirements. `EXPLAIN` now emits `LOGICAL_TIMESTAMP` and `FRONTIER_REQUIREMENT` notices for opt-in temporal contracts. See [M239_EXPLAIN_FRONTIER.md](M239_EXPLAIN_FRONTIER.md).
- [x] M240 Raw dataflow explain output for operator and exchange topology. `ExplainDataflowGraph.Exchanges` now exposes stage-boundary worker transport while preserving pipeline and subplan edges. See [M240_EXPLAIN_DATAFLOW.md](M240_EXPLAIN_DATAFLOW.md).
- [x] M241 Optimizer trace showing rule applications and rejected alternatives. `SQLQueryOptions.OptimizerTrace` is default-off, and optimizer rules can emit structured rejected alternatives through `RejectAlternative`. See [M241_OPTIMIZER_TRACE.md](M241_OPTIMIZER_TRACE.md).
- [x] M242 Per-operator update, batch, and frontier metrics. `SQLQueryOptions.OperatorMetrics` is default-off and emits an optional `QueryEvent.OperatorMetrics` sidecar. See [M242_OPERATOR_METRICS.md](M242_OPERATOR_METRICS.md).
- [x] M243 Arrangement memory metrics split by key, value, and trace history. `TypedTableAggregateArrangementStats` exposes `EstimatedKeyBytes`, `EstimatedValueBytes`, and `EstimatedTraceBytes` while preserving `EstimatedBytes`. See [M243_ARRANGEMENT_MEMORY_METRICS.md](M243_ARRANGEMENT_MEMORY_METRICS.md).
- [x] M244 Compaction debt metrics measured against the current logical frontier. `TypedTableAggregateArrangementStats.CompactionDebt` reports `max(0, SourceSequence-CompactedThrough)` without changing compaction behavior. See [M244_COMPACTION_DEBT.md](M244_COMPACTION_DEBT.md).
- [x] M245 Timestamp throughput and input-to-output latency metrics. `SQLTelemetry.ObserveSQLTimestamp` exports update/batch throughput counters, monotone logical timestamp gauges, and guarded input-to-output freshness latency. See [M245_TIMESTAMP_TELEMETRY.md](M245_TIMESTAMP_TELEMETRY.md).
- [x] M246 Per-object history-retention policies with bounded storage accounting. `FrontierRetentionRegistry` provides bounded per-frontier policies and caller-reported usage through an opt-in sidecar; see [M246_FRONTIER_RETENTION_POLICY.md](M246_FRONTIER_RETENTION_POLICY.md).
- [x] M247 Resume errors that identify when a requested frontier has expired. `FrontierRetentionExpiredError` preserves the old sentinel while exposing the frontier ID, requested as-of, and observed bounds; see [M247_FRONTIER_EXPIRY_ERRORS.md](M247_FRONTIER_EXPIRY_ERRORS.md).
- [x] M248 Reusable maintained-result cache for identical read expressions. NewMaintainedResultCache coalesces bounded concurrent misses while preserving source-version and dependency invalidation fences; see M248_MAINTAINED_RESULT_CACHE.md.
- [x] M249 Consistency fencing between a point read and a subsequent subscription; see [M249_READ_FENCE.md](M249_READ_FENCE.md).
- [x] M250 Temporal join alignment that waits for both input frontiers. `SQLTemporalJoinFrontierAlignment` waits for both input barriers concurrently and returns a safe common frontier; see [M250_TEMPORAL_JOIN_FRONTIER_ALIGNMENT.md](M250_TEMPORAL_JOIN_FRONTIER_ALIGNMENT.md).

## Tarantool: 50 Additional Ideas

- [x] T201 Per-space synchronous replication quorum for critical records only. `hatReplication.PerSpaceWriteQuorum` applies durable quorum acknowledgements only to explicitly configured critical spaces and leaves unconfigured spaces asynchronous; see [T201_PER_SPACE_SYNC_QUORUM.md](T201_PER_SPACE_SYNC_QUORUM.md).
- [x] T202 Automatic leader election for a replica set. `hatReplication.ReplicaSetLeaderElection` provides opt-in quorum-gated heartbeat expiry, deterministic candidate selection, and generation-fenced commit; see [T202_REPLICA_SET_LEADER_ELECTION.md](T202_REPLICA_SET_LEADER_ELECTION.md).
- [x] T203 Strict leader fencing against stale writers after failover. `hatReplication.ReplicaSetLeaderWriteFence` gates exact node/term/fencing-token credentials and serializes failover advancement with local write callbacks; see [T203_LEADER_WRITE_FENCE.md](T203_LEADER_WRITE_FENCE.md).
- [x] T204 Supervised failover with explicit operator override and recovery state. `hatReplication.SupervisedFailoverCoordinator` requires named approval/override, fences commit generations, tracks caught-up recovery or failure, and requires explicit reset; see [T204_SUPERVISED_FAILOVER.md](T204_SUPERVISED_FAILOVER.md).
- [x] T205 LSN-based replication lag and apply-throughput metrics. `hatReplication.ReplicationProgressMetrics` is opt-in, bounded, monotone, and derives LSN/byte rates from caller-fed observations; see [T205_REPLICATION_PROGRESS_METRICS.md](T205_REPLICATION_PROGRESS_METRICS.md) and [BENCHMARK.md#t205-replication-progress-metrics](BENCHMARK.md#t205-replication-progress-metrics).
- [x] T206 Deterministic replica bootstrap and join workflow. `hatReplication.ReplicaJoinAdmission` validates bounded node/address identities, chooses a source deterministically, requires an active caught-up `SnapshotWALBootstrapCoordinator`, and fences membership publication by topology generation; see [T206_REPLICA_BOOTSTRAP_JOIN.md](T206_REPLICA_BOOTSTRAP_JOIN.md) and [BENCHMARK.md#t206-deterministic-replica-bootstrap-and-join](BENCHMARK.md#t206-deterministic-replica-bootstrap-and-join).
- [x] T207 Replica eviction, rejoin, and stale-state recovery protocol. `hatReplication.ReplicaJoinAdmission` now retains bounded eviction epochs, blocks ordinary joins for tombstoned identities, and requires an exact active caught-up bootstrap plus topology generation for rejoin; see [T207_REPLICA_EVICTION_RECOVERY.md](T207_REPLICA_EVICTION_RECOVERY.md) and [BENCHMARK.md#t207-replica-eviction-rejoin-and-stale-state-recovery](BENCHMARK.md#t207-replica-eviction-rejoin-and-stale-state-recovery).
- [x] T208 Anonymous replicas that do not participate in quorum decisions. `hatReplication.ReplicaJoinAdmission` defaults members to voters, supports explicit `ReplicaRoleAnonymous` joins, exports a detached role roster, and `ExecuteVoterWriteQuorum` rejects anonymous and unknown targets before callbacks; see [T208_ANONYMOUS_REPLICAS.md](T208_ANONYMOUS_REPLICAS.md) and [BENCHMARK.md#t208-anonymous-replicas-and-voter-only-quorum](BENCHMARK.md#t208-anonymous-replicas-and-voter-only-quorum).
- [x] T209 Relay/applier backpressure when a replica falls behind. `hatReplication.ApplierThrottle` provides an opt-in, allocation-free reservation schedule for ordered replica apply work; see [TU37_REPLICA_APPLIER_THROTTLE.md](TU37_REPLICA_APPLIER_THROTTLE.md) and [BENCHMARK.md#t209-relayapplier-backpressure](BENCHMARK.md#t209-relayapplier-backpressure).
- [x] T210 Master-master conflict hooks with source and sequence context. `hatReplication.ConflictPolicy.Hook` receives both immutable `ConflictVersion` values, including source IDs and per-source sequences, and can select a side, delegate to policy, or reject; see [T210_MASTER_MASTER_CONFLICT_HOOKS.md](T210_MASTER_MASTER_CONFLICT_HOOKS.md) and [BENCHMARK.md#t210-master-master-conflict-hooks](BENCHMARK.md#t210-master-master-conflict-hooks).
- [x] T211 Configurable WAL synchronization modes with durability reporting. `hatJournal.SpaceSyncPolicyRegistry` provides bounded per-space periodic, immediate, and disabled policies with explicit durability semantics; see [TU34_SPACE_WAL_SYNC_POLICY.md](TU34_SPACE_WAL_SYNC_POLICY.md) and [BENCHMARK.md#t-u34-per-space-wal-sync-policy-registry](BENCHMARK.md#t-u34-per-space-wal-sync-policy-registry).
- [x] T212 WAL retention and rotation policies tied to replica acknowledgments. `hatCache.CommandJournalOptions.ReplicaRetentionCapacity` enables a bounded runtime registry; `AcknowledgeReplicaThrough` gates segmented deletion and legacy compaction at the slowest replica, while capacity `0` remains the default. See [T212_WAL_RETENTION_REPLICA_ACKS.md](T212_WAL_RETENTION_REPLICA_ACKS.md) and [BENCHMARK.md#t212-replica-acknowledgment-wal-retention](BENCHMARK.md#t212-replica-acknowledgment-wal-retention).
- [x] T213 Scheduled snapshots with checkpoint manifests and atomic publication. `hatCache.ScheduledSnapshotter` periodically reuses the resumable snapshot exporter, preserves failed-copy checkpoints, and atomically publishes a durable digest/journal-coordinate manifest; see [T213_SCHEDULED_SNAPSHOTS.md](T213_SCHEDULED_SNAPSHOTS.md) and [BENCHMARK.md#t213-scheduled-snapshots-with-checkpoint-manifests](BENCHMARK.md#t213-scheduled-snapshots-with-checkpoint-manifests).
- [x] T214 Streaming snapshots for replicas without a shared filesystem. `hatCache.StreamCommandJournalSnapshot` forwards authenticated snapshot bytes directly to an `io.Writer`, validates sequence/format/length/digest headers, and avoids local temporary files; see [T214_STREAMING_SNAPSHOTS.md](T214_STREAMING_SNAPSHOTS.md) and [BENCHMARK.md#t214-streaming-snapshots-without-a-shared-filesystem](BENCHMARK.md#t214-streaming-snapshots-without-a-shared-filesystem).
- [x] T215 Per-space memtx versus on-disk storage policy. `hatDataStructure.StorageSpace` keeps the zero-value/default policy in memory and provides an explicit on-disk spill/reopen policy backed by `SpillableArrangement`; see [T215_PER_SPACE_STORAGE_POLICY.md](T215_PER_SPACE_STORAGE_POLICY.md) and [BENCHMARK.md#t215-per-space-memtx-versus-on-disk-storage-policy](BENCHMARK.md#t215-per-space-memtx-versus-on-disk-storage-policy).
- [x] T216 Vinyl-style run compaction scheduling and space accounting. `hatDataStructure.StorageSpaceCompactionScheduler` is caller-driven and default-off; maintained per-space stale-byte counters make compaction debt observable without scanning entries. See [T216_STORAGE_COMPACTION_SCHEDULER.md](T216_STORAGE_COMPACTION_SCHEDULER.md) and [BENCHMARK.md#t216-storage-compaction-scheduler](BENCHMARK.md#t216-storage-compaction-scheduler).
- [x] T217 In-memory columnar storage for analytical spaces. `hatDataStructure.ColumnarSpace` provides fixed typed buffers, lazy null bitmaps, packed booleans, offset-backed strings/bytes, atomic row validation, and detached snapshots without changing existing defaults. See [T217_IN_MEMORY_COLUMNAR_SPACE.md](T217_IN_MEMORY_COLUMNAR_SPACE.md) and [BENCHMARK.md#t217-in-memory-columnar-space](BENCHMARK.md#t217-in-memory-columnar-space).
- [x] T218 Multi-part TREE indexes with ordered prefix and range scans. `hatDataStructure.MultiPartTreeIndex` publishes immutable sorted parts, locates bounds per part, and k-way merges the results; it is opt-in and does not replace the existing single-vector `OrderedIndex`. See [T218_MULTI_PART_TREE_INDEX.md](T218_MULTI_PART_TREE_INDEX.md) and [BENCHMARK.md#t218-multi-part-tree-index](BENCHMARK.md#t218-multi-part-tree-index).
- [x] T219 HASH indexes for constant-time exact lookups. `hatDataStructure.PackedHashIndex` adds an immutable flat open-addressed exact-match table for read-heavy snapshots; the existing mutable `HashIndex` remains unchanged. See [T219_PACKED_HASH_INDEX.md](T219_PACKED_HASH_INDEX.md) and [BENCHMARK.md#t219-packed-hash-index](BENCHMARK.md#t219-packed-hash-index).
- [x] T220 RTREE indexes for spatial bounding-box searches. Already adopted by `RTree`, `PackedRTree`, `MutablePackedRTree`, `RTreeSpaceCatalog`, and the SQL `RTreeSpatialSource`; see [TT021_PACKED_RTREE.md](TT021_PACKED_RTREE.md), [TU25_RTREE_SPACE_CATALOG.md](TU25_RTREE_SPACE_CATALOG.md), and [TR027_RTREE_SPATIAL_INDEX.md](TR027_RTREE_SPATIAL_INDEX.md).
- [x] T221 BITSET indexes for low-cardinality membership predicates. Already adopted by the typed `hatDataStructure.BitmapIndex[K]`; see [TR026_BITMAP_INDEX.md](TR026_BITMAP_INDEX.md).
- [x] T222 Multikey indexes over array-valued fields. Already adopted by the string and typed tuple multikey indexes; see [SQL_MULTIKEY_INDEX.md](SQL_MULTIKEY_INDEX.md) and [TU23_TYPED_MULTIKEY_INDEX.md](TU23_TYPED_MULTIKEY_INDEX.md).
- [x] T223 Functional indexes over derived field expressions. Already adopted by the generation-checked `MaterializedSource.BuildFunctionalIndex`; see [TR023_FUNCTIONAL_INDEX.md](TR023_FUNCTIONAL_INDEX.md).
- [x] T224 Partial indexes restricted by a validated predicate. Already adopted by the conditional index catalog with fenced rebuild and atomic replacement; see [TU24_CONDITIONAL_INDEX_CATALOG.md](TU24_CONDITIONAL_INDEX_CATALOG.md).
- [x] T225 Covering indexes that return projected fields without row fetches. Already adopted for opt-in materialized-source equality indexes; see [TR024_COVERING_INDEX.md](TR024_COVERING_INDEX.md).
- [x] T226 Explicit index hints with planner diagnostics. Already adopted with force/forbid modes and deterministic candidate inspection; see [TU26_INDEX_STRATEGY_INSPECTION.md](TU26_INDEX_STRATEGY_INSPECTION.md).
- [x] T227 Per-field nullability, type, and constraint validation. Already adopted by `hatSchema.Column`, `hatSchema.Schema.Validate`, `hatSchema.ValidateRows`, and named NOT NULL, UNIQUE, CHECK, and foreign-key constraints.
- [x] T228 Tuple-format schema versions with compatible readers. Existing `hatSchema.Schema.Version`, reversible migrations, and `CheckRollingCompatibility` provide explicit versioning and conservative rolling-reader checks. See `hat/hatSchema/schema.go`, `hat/hatSchema/compatibility.go`, and `hat/hatSchema/rolling_schema.go`.
- [x] T229 Before-replace triggers for validation and conflict policy. `hatDataStructure.MemtxTable` exposes an opt-in typed `BeforeReplace` hook that can reject or normalize atomic inserts/upserts. See [TT029_BEFORE_REPLACE.md](TT029_BEFORE_REPLACE.md).
- [x] T230 On-replace changefeed hooks with old and new tuple images. `hatDataStructure.MemtxTable` emits an ordered opt-in `OnReplace` event after successful inserts/upserts with old and new typed images. See [TT030_ON_REPLACE_CHANGEFEED.md](TT030_ON_REPLACE_CHANGEFEED.md).
- [x] T231 After-replace audit hooks with transaction identity. `hatDataStructure.MemtxTable` emits an opt-in `AfterReplace` audit callback with a table-local transaction identity and the committed old/new event. See [TT031_AFTER_REPLACE_AUDIT.md](TT031_AFTER_REPLACE_AUDIT.md).
- [ ] T232 Atomic transaction scopes with nested rollback boundaries.
- [ ] T233 MVCC transactions that permit cooperative yields.
- [ ] T234 Early conflict detection for competing transactional writes.
- [ ] T235 Cooperative fiber workers for nonblocking application tasks.
- [ ] T236 Low-overhead mailbox channels between independent workers.
- [ ] T237 Connection pools with health checks and reconnect backoff.
- [ ] T238 Batched binary protocol requests with ordered responses.
- [x] T239 Prepared request templates that reuse encoded field metadata. `hatPeer.CompactRequestTemplate` copies command metadata once and supports caller-buffer reuse without changing the wire format; the paired benchmark measured `23-25 ns/op`, `0 B/op`, and `0 allocs/op` versus direct marshal at `36-38 ns/op`, `32 B/op`, and `1 alloc/op`; see [T239_COMPACT_REQUEST_TEMPLATE.md](T239_COMPACT_REQUEST_TEMPLATE.md).
- [x] T240 Request cancellation and deadline propagation through the protocol. `CompactPeerSessionOptions.EnableRequestCancellation` sends a best-effort reserved request that cancels the matching remote handler context; the default remains off. See [T240_COMPACT_REQUEST_CANCELLATION.md](T240_COMPACT_REQUEST_CANCELLATION.md).
- [ ] T241 Role-based authentication and per-space authorization.
- [ ] T242 Append-only audit logging for administrative and data operations.
- [x] T243 Mutual TLS authentication with certificate rotation; see [T243_MTLS_CERTIFICATE_ROTATION.md](T243_MTLS_CERTIFICATE_ROTATION.md).
- [ ] T244 Queue task delay and deadline scheduling.
- [ ] T245 Queue visibility timeouts for worker crash recovery.
- [ ] T246 Priority queues with starvation bounds.
- [ ] T247 Queue task deduplication by client-supplied identity.
- [ ] T248 Retry counters and dead-letter routing for failed tasks.
- [ ] T249 Queue capacity, age, retry, and consumer-lag metrics.
- [ ] T250 Gap-safe sequence allocation with durable current value.
## C204 Status

C204 is adopted. Idempotency keys now propagate from async journal entries
through incremental projections into refreshed materialized-view status. See
[C204_PROJECTION_IDEMPOTENCY.md](C204_PROJECTION_IDEMPOTENCY.md) and the
paired measurements in [BENCHMARK.md](BENCHMARK.md#c204-projection-idempotency-metadata).

### C234 Query stage profiler

- [x] Added bounded explicit stage aggregation for SQL profiling.
- Implementation: `hat/hatSql/ch234_stage_profiler.go` and
  `hat/hatSql/ch234_stage_profiler_test.go`.
- Benchmark and tradeoff: `C234_QUERY_STAGE_PROFILER.md`.
