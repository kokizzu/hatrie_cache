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
- [x] C219 Per-group `LIMIT BY` execution with bounded memory. Already present in `hatSql/limit_by.go`, including streaming, top-N, external-sort, composite-key, and dedicated benchmark coverage.
- [x] C220 Post-window `QUALIFY` filtering over projected window rows; selected window aliases are evaluated before `DISTINCT`, `ORDER BY`, and `LIMIT`; see [C220_QUALIFY.md](C220_QUALIFY.md).
- [x] C221 `WITH TIES` limit semantics for deterministic boundary results; see [LIMIT_WITH_TIES.md](LIMIT_WITH_TIES.md) and `make test-limit-with-ties-local-clean`.
- [x] C222 Approximate top-K aggregation with mergeable bounded state; importable `hatCache.TopK.Merge`, HAG1 aggregate-state round trips, and `HatTrie.MergeTopK` provide bounded partition-state union without changing existing command defaults. SQL planner integration remains open.
- [x] C223 Mergeable approximate distinct and quantile aggregate states; importable HLL, Count-Min, and TDigest states support validated partition merges and compact transfer. SQL aggregate `State`/`Merge` syntax remains open.
- [x] C223a Mergeable HyperLogLog partial state; `HyperLogLog.Merge` combines same-precision partition states with per-register maxima, supports zero-value receiver adoption, and rejects invalid or mismatched states without mutation. SQL `State`/`Merge` syntax remains open under C223.
- [x] C223b Compact mergeable TDigest aggregate state; fixed-width centroid payloads use HAG1 framing, validate compression/count/order/finite values, and support atomic merge-from-wire without changing existing defaults. An existing large-input centroid-bound mismatch remains a separate compaction task.
- [x] C223b Mergeable Count-Min Sketch partial state; exported `CountMinSketch` snapshots merge same-shape counter matrices with saturating addition, and `HatTrie.MergeCountMinSketch` imports an owned state without changing existing command defaults. SQL `State`/`Merge` syntax remains open under C223.
- [x] C223c Versioned partial aggregate envelopes; importable HLL and Count-Min state APIs now use bounded, checksummed HAG1 frames with compact raw-register/counter payloads, explicit kind/version metadata, and strict decoder validation. SQL `State`/`Merge` integration remains open under C223.
- [x] C224 `argMax` and `argMin` aggregate states with deterministic tie handling; see [C224_ARGMAX_ARGMIN_STATE.md](C224_ARGMAX_ARGMIN_STATE.md).
- [x] C225 Incremental window-frame state for repeated ordered windows; see [INCREMENTAL_FRAME_WINDOW.md](INCREMENTAL_FRAME_WINDOW.md) and `make test-m065d-incremental-frame-window`.
- [x] C226 Grace-hash join spilling with bounded disk runs; the opt-in partitioned path is covered by [C229_JOIN_OVERFLOW_POLICY.md](C229_JOIN_OVERFLOW_POLICY.md), [CHU25_QUERY_SPILL_QUOTA.md](CHU25_QUERY_SPILL_QUOTA.md), and `make test-chu08-join-overflow`.
- [x] C227 External aggregation spilling with merge-time memory limits; `SQLQueryOptions.MaxGroupMergeBytes` bounds the decoded spill-reader frontier, stays off by default, and cleans temporary files on rejection. See [C227_GROUP_MERGE_BUDGET.md](C227_GROUP_MERGE_BUDGET.md) and [BENCHMARK.md#c227-external-group-merge-memory-budget](BENCHMARK.md#c227-external-group-merge-memory-budget).
- [x] C228 External sort spilling with stable run ordering; the bounded external `ORDER BY` path captures input ordinals for deterministic tie ordering, cleans spill files on success and quota failure, and is covered by [CHU02_EXTERNAL_ORDER_SPILL.md](CHU02_EXTERNAL_ORDER_SPILL.md), `make test-chu02-c242`, `make race-chu02-c242`, and `make benchmark-chu02-c242`. The opt-in spill path is slower and allocates more on small inputs, so materialized sorting remains the default.
- [x] C229 Explicit join overflow policy for auto, reject, and bounded spill; truncation remains intentionally unsupported to preserve SQL correctness. See [C229_JOIN_OVERFLOW_POLICY.md](C229_JOIN_OVERFLOW_POLICY.md) and [BENCHMARK.md#c229-join-overflow-policy](BENCHMARK.md#c229-join-overflow-policy).
- [x] C230 Memory-overcommit wait queues before query cancellation; `SQLQueryOptions.ClusterAdmission` now waits on the existing bounded CPU/memory admission queue before taking source snapshots or locks, honors context/timeout cancellation, and releases its lease on every query exit. See [C230_MEMORY_OVERCOMMIT.md](C230_MEMORY_OVERCOMMIT.md), `make test-c230-memory-overcommit`, `make race-c230-memory-overcommit`, and `make benchmark-c230-memory-overcommit`.
- [x] C231 Workload groups with per-class concurrency and memory budgets. `SQLWorkloadClass` now supports per-class concurrency and memory caps, with `AcquireWithMemory`, `RunWithMemory`, and `ClassStats`; see [C231_WORKLOAD_GROUPS.md](C231_WORKLOAD_GROUPS.md) and [BENCHMARK.md](BENCHMARK.md#c231-sql-workload-groups).
- [x] C232 Query-complexity limits for rows, joins, and intermediate results.
- [x] C233 Per-query CPU-time budgets with cooperative cancellation; see [C233_QUERY_CPU_BUDGET.md](C233_QUERY_CPU_BUDGET.md) and [BENCHMARK.md](BENCHMARK.md#c233-query-cpu-time-budget).
- [x] C234 [Query profiler records for stage time, bytes, and allocations](C234_QUERY_PROFILER.md).
- [x] C235 [Read/write task profiler aggregation by table part and column](C235_READ_WRITE_PROFILER.md).
- [x] C236 Explain output for data-skipping-index decisions and rejected marks; EXPLAIN ANALYZE now exposes bounded per-mark skip/scan decisions, reasons, complete examined/rejected counts, and truncation state for columnar pruning paths. See C236_EXPLAIN_SKIP_DECISIONS.md and `make test-c236-explain`.
- [x] C237 Explain output for projection selection and estimated I/O cost; EXPLAIN now reports selected columnar fields, predicate/output field roles, and bounded selected-batch read estimates. See [C237_EXPLAIN_PROJECTION.md](C237_EXPLAIN_PROJECTION.md) and `make test-c237-projection`.
- [x] C238 Mutation queue progress with ready/pending/blocked/running/completed/failed/remaining counts and queue-lifetime elapsed/estimated-remaining nanoseconds. See [C238_MUTATION_QUEUE_PROGRESS.md](C238_MUTATION_QUEUE_PROGRESS.md) and `make test-c238-progress`.
- [x] C239 Part-merge backlog, amplification, and age metrics via `TypedTable.PartMergeMetrics()`. See [C239_PART_MERGE_METRICS.md](C239_PART_MERGE_METRICS.md) and `make test-c239-merge-metrics`.
- [x] C240 Read-only backup database attachment for querying snapshot/checkpoint/repository parts without publishing a writable restore. See [C240_READ_ONLY_BACKUP_ATTACHMENT.md](C240_READ_ONLY_BACKUP_ATTACHMENT.md) and [BENCHMARK.md](BENCHMARK.md#c240-read-only-backup-attachment).
- [x] C241 Incremental backup chunk deduplication across snapshots. See [C241_INCREMENTAL_BACKUP_CHUNK_DEDUP.md](C241_INCREMENTAL_BACKUP_CHUNK_DEDUP.md) and [BENCHMARK.md](BENCHMARK.md#c241-incremental-backup-chunk-deduplication).
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
- [x] M206 Upsert envelopes that expose a stable key and current row image. `hatSql.UpsertEnvelope` normalizes CDC/Debezium changes, preserves the current row or delete tombstone, and caches canonical Debezium keys; see [M206_UPSERT_ENVELOPES.md](M206_UPSERT_ENVELOPES.md) and [BENCHMARK.md#m206-stable-upsert-envelopes](BENCHMARK.md#m206-stable-upsert-envelopes).
- [x] M207 Debezium envelopes with before/after images and operation type. `hatSql.DebeziumChangefeed` requires declared unique key columns, emits snapshot/create/update/delete payloads, preserves subscription frontier metadata, and rejects ambiguous multiplicity; the optimized adapter measured about 16x lower latency, 18x lower allocated bytes, and 10x fewer allocations than its full-state-copy baseline; see [M207_DEBEZIUM_CHANGEFEED.md](M207_DEBEZIUM_CHANGEFEED.md).
- [x] M208 Differential multiplicity folding for insert/delete update streams. `hatSql.ConsolidateQuerySubscriptionDeltas` folds equal complete rows with overflow-safe signed counts; `hatSql.DebeziumChangefeed` retries only rejected duplicate/unsupported-multiplicity batches, preserving the normal fast path and distinct-row duplicate-key rejection; see [M208_DIFFERENTIAL_MULTIPLICITY.md](M208_DIFFERENTIAL_MULTIPLICITY.md) and [BENCHMARK.md#m208-differential-multiplicity-folding](BENCHMARK.md#m208-differential-multiplicity-folding).
- [x] M209 Monotone logical timestamp frontiers for read and stream APIs. `hatSql.SQLLogicalFrontier` is an opt-in, allocation-free high-watermark guard for historical SQL reads and query subscriptions; failed reads and rejected regressions do not advance it. See [M209_LOGICAL_FRONTIERS.md](M209_LOGICAL_FRONTIERS.md) and [BENCHMARK.md#m209-monotone-logical-frontiers](BENCHMARK.md#m209-monotone-logical-frontiers).
- [x] M210 Historical `AS OF` reads against retained logical state. `hatSql.SQLRetainedState` provides bounded copy-on-write exact frontier views, implements the existing historical resolver/provider contracts, and preserves atomic publish plus retention-boundary errors; see [M210_RETAINED_STATE.md](M210_RETAINED_STATE.md) and [BENCHMARK.md#m210-retained-logical-state](BENCHMARK.md#m210-retained-logical-state).
- [x] M211 Explicit rejection of reads before `since` or at/after `upper` frontiers. `SQLQueryOptions` now supports an opt-in `[since, upper)` admission interval, including signed snapshot-token reads, with rejection before provider access across materialized, stream, offset-page, and keyset-page APIs; see [M211_AS_OF_BOUNDS.md](M211_AS_OF_BOUNDS.md) and [BENCHMARK.md#m211-explicit-as-of-bounds](BENCHMARK.md#m211-explicit-as-of-bounds).
- [x] M212 Logical compaction that advances retained history without rewriting live state; see [M212_LOGICAL_COMPACTION.md](M212_LOGICAL_COMPACTION.md) and [BENCHMARK.md#m212-logical-compaction](BENCHMARK.md#m212-logical-compaction).
- [x] M213 Consolidation of equal updates before forwarding to downstream consumers. `QuerySubscriptionDeltaBatch.Consolidate` normalizes unmarked batches, generated subscription batches skip duplicate work with an internal marker, and `DebeziumChangefeed.Apply` enforces the boundary; see [M213_DIFFERENTIAL_CONSOLIDATION.md](M213_DIFFERENTIAL_CONSOLIDATION.md) and [BENCHMARK.md#m213-differential-batch-consolidation](BENCHMARK.md#m213-differential-batch-consolidation).
- [x] M214 Arrangement reuse across indexes and compatible query plans; the opt-in `TypedTableSortedArrangements` registry shares exact or longer-prefix sorted state with reference-counted leases. See [M214_SORTED_ARRANGEMENT_REUSE.md](M214_SORTED_ARRANGEMENT_REUSE.md) and [BENCHMARK.md](BENCHMARK.md#m214-sorted-arrangement-reuse).
- [x] M215 Delta-join maintenance for high-churn join inputs; opt-in `ApplyLeftDeltas`/`ApplyRightDeltas` emits only affected signed pair changes while preserving the existing full-state APIs. See [M215_DELTA_JOIN_MAINTENANCE.md](M215_DELTA_JOIN_MAINTENANCE.md) and [BENCHMARK.md](BENCHMARK.md#m215-delta-join-maintenance).
- [ ] M216 Incremental top-K arrangements with bounded replacement state.
- [ ] M217 Indexes that store complete maintained view results for point lookups.
- [ ] M218 Planner selection of point lookup versus arrangement scan.
- [ ] M219 Background index creation with observable build frontier.
- [ ] M220 Safe index removal after dependent readers drain.
- [ ] M221 Isolated compute clusters with independent resource budgets.
- [ ] M222 Replicated compute workers for highly available maintained indexes.
- [ ] M223 Hydration state machines that distinguish cold, hydrating, and ready views.
- [ ] M224 Hydration progress and estimated remaining work metrics.
- [ ] M225 Persisted shard leases that prevent duplicate state ownership.
- [ ] M226 Durable consensus metadata for state shard and frontier ownership.
- [ ] M227 Source snapshot offsets coupled atomically to the first live frontier.
- [ ] M228 Exactly-once source restart from a committed source offset.
- [ ] M229 Source schema evolution with additive field compatibility checks.
- [ ] M230 Source backpressure based on downstream frontier lag.
- [ ] M231 Exactly-once upsert sinks with durable output identities.
- [ ] M232 Sink progress checkpoints coupled to emitted frontier messages.
- [ ] M233 Sink retry and deduplication for disconnected output connections.
- [ ] M234 Sink backpressure and bounded pending-output memory.
- [ ] M235 Dependency graph invalidation for affected indexes and views only.
- [ ] M236 On-demand refresh of only invalidated maintained objects.
- [ ] M237 Lazy hydration triggered by the first reader with cancellation support.
- [ ] M238 Explain output for filter pushdown and arrangement reuse.
- [ ] M239 Explain output for logical timestamp and frontier requirements.
- [ ] M240 Raw dataflow explain output for operator and exchange topology.
- [ ] M241 Optimizer trace showing rule applications and rejected alternatives.
- [ ] M242 Per-operator update, batch, and frontier metrics.
- [ ] M243 Arrangement memory metrics split by key, value, and trace history.
- [ ] M244 Compaction debt metrics measured against the current logical frontier.
- [ ] M245 Timestamp throughput and input-to-output latency metrics.
- [ ] M246 Per-object history-retention policies with bounded storage accounting.
- [ ] M247 Resume errors that identify when a requested frontier has expired.
- [ ] M248 Reusable maintained-result cache for identical read expressions.
- [x] M249 Consistency fencing between a point read and a subsequent subscription; see [M249_READ_FENCE.md](M249_READ_FENCE.md).
- [ ] M250 Temporal join alignment that waits for both input frontiers.

## Tarantool: 50 Additional Ideas

- [x] T201 Per-space synchronous replication quorum for critical records only. See [T201_PER_SPACE_WRITE_QUORUM.md](T201_PER_SPACE_WRITE_QUORUM.md) and [BENCHMARK.md](BENCHMARK.md#t201-per-space-write-quorum).
- [x] T202 Automatic leader election for a replica set. Implemented in
  [`T202_AUTOMATIC_LEADER_ELECTION.md`](T202_AUTOMATIC_LEADER_ELECTION.md),
  with an opt-in heartbeat requirement and caller-owned refresh loop.
- [x] T203 Strict leader fencing against stale writers after failover; see [T203_LEADER_FENCING.md](T203_LEADER_FENCING.md) and [BENCHMARK.md](BENCHMARK.md#t203-strict-leader-fencing).
- [x] T204 Supervised failover with explicit operator override and recovery state; see [T204_SUPERVISED_FAILOVER.md](T204_SUPERVISED_FAILOVER.md) and [BENCHMARK.md](BENCHMARK.md#t204-supervised-failover).
- [x] T205 LSN-based replication lag and apply-throughput metrics; see [T205_LSN_REPLICATION_METRICS.md](T205_LSN_REPLICATION_METRICS.md) and [BENCHMARK.md](BENCHMARK.md#t205-lsn-replication-metrics).
- [x] T206 Deterministic replica bootstrap and join workflow; see [T206_DETERMINISTIC_REPLICA_BOOTSTRAP.md](T206_DETERMINISTIC_REPLICA_BOOTSTRAP.md) and [BENCHMARK.md](BENCHMARK.md#t206-deterministic-replica-bootstrap).
- [x] T207 Replica eviction, rejoin, and stale-state recovery protocol; see [T207_REPLICA_RECOVERY.md](T207_REPLICA_RECOVERY.md) and [BENCHMARK.md](BENCHMARK.md#t207-replica-eviction-rejoin-and-stale-state-recovery).
- [x] T208 Anonymous replicas that do not participate in quorum decisions; see [T208_ANONYMOUS_REPLICAS.md](T208_ANONYMOUS_REPLICAS.md) and [BENCHMARK.md](BENCHMARK.md#t208-anonymous-replicas).
- [x] T209 Relay/applier backpressure when a replica falls behind; see [T209_RELAY_APPLIER_BACKPRESSURE.md](T209_RELAY_APPLIER_BACKPRESSURE.md) and [BENCHMARK.md](BENCHMARK.md#t209-relayapplier-backpressure).
- [x] T210 Master-master conflict hooks with source and sequence context; see [T210_MASTER_MASTER_CONFLICT_HOOKS.md](T210_MASTER_MASTER_CONFLICT_HOOKS.md) and [BENCHMARK.md](BENCHMARK.md#t210-master-master-conflict-hooks).
- [x] T211 Configurable WAL synchronization modes with durability reporting; see [T211_CONFIGURABLE_WAL_SYNC.md](T211_CONFIGURABLE_WAL_SYNC.md).
- [x] T212 WAL retention and rotation policies tied to replica acknowledgments; see [T212_WAL_REPLICA_RETENTION.md](T212_WAL_REPLICA_RETENTION.md) and [BENCHMARK.md](BENCHMARK.md#t212-wal-replica-acknowledgement-retention).
- [x] T213 Scheduled snapshots with checkpoint manifests and atomic publication; see [T213_SCHEDULED_SNAPSHOTS.md](T213_SCHEDULED_SNAPSHOTS.md) and [BENCHMARK.md](BENCHMARK.md#t213-scheduled-snapshot-checkpoints).
- [x] T214 Streaming snapshots for replicas without a shared filesystem; see [T214_SNAPSHOT_STREAMING.md](T214_SNAPSHOT_STREAMING.md) and [BENCHMARK.md](BENCHMARK.md#t214-streaming-snapshots).
- [x] T215 Per-space memtx versus on-disk storage policy; see [T215_PER_SPACE_STORAGE_POLICY.md](T215_PER_SPACE_STORAGE_POLICY.md) and [BENCHMARK.md](BENCHMARK.md#t215-per-space-storage-policy).
- [x] T216 Vinyl-style run compaction scheduling and space accounting; see [T216_COMPACTION_SCHEDULING.md](T216_COMPACTION_SCHEDULING.md) and [BENCHMARK.md](BENCHMARK.md#t216-vinyl-style-compaction-scheduling).
- [x] T217 Columnar batch ingest for analytical typed tables; see [T217_COLUMNAR_BATCH_INGEST.md](T217_COLUMNAR_BATCH_INGEST.md) and [BENCHMARK.md](BENCHMARK.md#t217-columnar-batch-ingest).
- [x] T218 Multi-part TREE indexes with ordered prefix and range scans; see [T218_MULTI_PART_TREE_INDEX.md](T218_MULTI_PART_TREE_INDEX.md) and [BENCHMARK.md](BENCHMARK.md#t218-multi-part-tree-index-prefix-scans).
- [x] T219 HASH indexes for constant-time exact lookups; see [T219_HASH_INDEX.md](T219_HASH_INDEX.md) and [BENCHMARK.md](BENCHMARK.md#t219-typed-hash-index).
- [x] T220 RTREE indexes for spatial bounding-box searches; see [T220_RTREE_INDEX.md](T220_RTREE_INDEX.md), [TR027_RTREE_SPATIAL_INDEX.md](TR027_RTREE_SPATIAL_INDEX.md), and [BENCHMARK.md](BENCHMARK.md#t220-r-tree-spatial-index).
- [x] T221 BITSET indexes for low-cardinality membership predicates; see [T221_BITMAP_INDEX.md](T221_BITMAP_INDEX.md) and [BENCHMARK.md](BENCHMARK.md#t221-typed-bitsetbitmap-index).
- [x] T222 Multikey indexes over array-valued fields; existing SQL JSON and typed tuple implementations were reverified with focused tests, race/vet checks, and fresh lookup/build benchmarks. See [T222_MULTIKEY_INDEX.md](T222_MULTIKEY_INDEX.md) and [BENCHMARK.md](BENCHMARK.md#t222-multikey-array-indexes).
- [x] T223 Functional indexes over derived field expressions; existing typed, conditional, materialized SQL, and `LOWER(...)` expression paths were reverified with focused tests, race/vet checks, and fresh before/after benchmarks. See [T223_FUNCTIONAL_INDEX.md](T223_FUNCTIONAL_INDEX.md) and [BENCHMARK.md](BENCHMARK.md#t223-functional-indexes-over-derived-expressions).
- [x] T224 Partial indexes restricted by a validated predicate; existing conditional functional/catalog and SQL JSON partial-index paths were reverified with focused tests, race/vet checks, and fresh selective-refresh benchmarks. See [T224_PARTIAL_INDEX.md](T224_PARTIAL_INDEX.md) and [BENCHMARK.md](BENCHMARK.md#t224-partial-indexes-with-validated-predicates).
- [x] T225 Covering indexes that return projected fields without row fetches; existing TR-024 materialized SQL covering indexes were reverified with resolver/query tests, race/vet checks, and fresh projection benchmarks. See [T225_COVERING_INDEX.md](T225_COVERING_INDEX.md) and [BENCHMARK.md](BENCHMARK.md#t225-covering-indexes-for-projected-fields).
- [x] T226 Explicit index hints with planner diagnostics; existing `FORCE`/`FORBID` strategy hints and deterministic `ExplainSQLIndexStrategy` inspection were reverified with focused tests, race/vet checks, and a fresh five-sample benchmark. See [T226_INDEX_HINTS.md](T226_INDEX_HINTS.md), [TU26_INDEX_STRATEGY_INSPECTION.md](TU26_INDEX_STRATEGY_INSPECTION.md), and [BENCHMARK.md](BENCHMARK.md#t226-explicit-index-hints-and-planner-diagnostics).
- [x] T227 Per-field nullability, type, and constraint validation; `hatSchema.ValidateFieldValue` now enforces declared Go value shapes, enum membership, and `NOT NULL` before existing row constraints, with focused, race, vet, and before/after benchmark coverage. See [T227_FIELD_VALIDATION.md](T227_FIELD_VALIDATION.md), [SCHEMA_CONSTRAINTS.md](SCHEMA_CONSTRAINTS.md), and [BENCHMARK.md](BENCHMARK.md#t227-per-field-schema-validation).
- [x] T228 Tuple-format schema versions with compatible readers; `TupleFormatReader` supports validated additive prefix evolution, defaults/generators/nullability for missing target suffixes, and strict source/version checks. See [T228_TUPLE_FORMAT_COMPATIBILITY.md](T228_TUPLE_FORMAT_COMPATIBILITY.md), [TUPLE_FORMAT_NEGOTIATION.md](TUPLE_FORMAT_NEGOTIATION.md), and [BENCHMARK.md](BENCHMARK.md#t228-tuple-format-compatible-readers).
- [x] T229 Before-replace triggers for validation and conflict policy; `SpaceOptions.BeforeReplace` validates copied old/new values for memtx and Vinyl while preserving the default fast path. See [T229_BEFORE_REPLACE.md](T229_BEFORE_REPLACE.md) and [BENCHMARK.md](BENCHMARK.md#t229-before-replace-triggers).
- [x] T230 On-replace changefeed hooks with old and new tuple images; `SpaceOptions.OnReplace` emits copied mutation images after successful memtx/Vinyl writes and can publish into `hatReplication.SpaceChangefeed`, while remaining disabled by default. See [T230_ON_REPLACE_CHANGEFEED.md](T230_ON_REPLACE_CHANGEFEED.md) and [BENCHMARK.md](BENCHMARK.md#t230-on-replace-changefeed-hooks).
- [x] T231 After-replace audit hooks with transaction identity; `SpaceOptions.AfterReplace` emits copied successful mutation images with monotonic per-space IDs while keeping the default path off. See [T231_AFTER_REPLACE_AUDIT.md](T231_AFTER_REPLACE_AUDIT.md) and [BENCHMARK.md](BENCHMARK.md#t231-after-replace-audit-hooks).
- [x] T232 Atomic transaction scopes with nested rollback boundaries; `Space.BeginTransaction` stages copied writes, supports nested merge/rollback boundaries, validates before mutation, and applies atomic memtx/Vinyl batches while retaining the default path. See [T232_SPACE_TRANSACTIONS.md](T232_SPACE_TRANSACTIONS.md) and [BENCHMARK.md](BENCHMARK.md#t232-atomic-space-transactions).
- [x] T233 MVCC transactions that permit cooperative yields; `Space.BeginMVCCTransaction` captures shared immutable memtx/LSM read metadata, nested scopes inherit repeatable reads, and `SpaceTransaction.Yield` cooperatively schedules with context cancellation. See [T233_MVCC_TRANSACTIONS.md](T233_MVCC_TRANSACTIONS.md) and [BENCHMARK.md](BENCHMARK.md#t233-mvcc-transactions).
- [x] T234 Early conflict detection for competing transactional writes via opt-in `Space.BeginConflictDetectingTransaction`; see [T234_TRANSACTION_CONFLICTS.md](T234_TRANSACTION_CONFLICTS.md) and [BENCHMARK.md](BENCHMARK.md#t234-early-transaction-conflict-detection).
- [x] T235 Cooperative fiber workers for nonblocking application tasks; see [T235_COOPERATIVE_WORKER_POOL.md](T235_COOPERATIVE_WORKER_POOL.md) and [BENCHMARK.md](BENCHMARK.md#t235-cooperative-fiber-worker-pool).
- [x] T236 Low-overhead mailbox channels between independent workers; see [T236_MAILBOX.md](T236_MAILBOX.md) and [BENCHMARK.md](BENCHMARK.md#t236-mailbox-channels).
- [x] T237 Connection pools with health checks and reconnect backoff; see [T237_CONNECTION_POOL_HEALTH.md](T237_CONNECTION_POOL_HEALTH.md) and [BENCHMARK.md](BENCHMARK.md#t237-connection-pool-health-checks).
- [x] T238 Batched binary protocol requests with ordered responses; see [T238_BATCHED_BINARY_PROTOCOL.md](T238_BATCHED_BINARY_PROTOCOL.md) and [BENCHMARK.md](BENCHMARK.md#t238-batched-binary-protocol-requests).
- [x] T239 Prepared request templates that reuse encoded field metadata. `hatPeer.CompactRequestTemplate` copies command metadata once and supports caller-buffer reuse without changing the wire format; the paired benchmark measured `23-25 ns/op`, `0 B/op`, and `0 allocs/op` versus direct marshal at `36-38 ns/op`, `32 B/op`, and `1 alloc/op`; see [T239_COMPACT_REQUEST_TEMPLATE.md](T239_COMPACT_REQUEST_TEMPLATE.md).
- [x] T240 Request cancellation and deadline propagation through the protocol. `CompactPeerSessionOptions.EnableRequestCancellation` sends a best-effort reserved request that cancels the matching remote handler context; the default remains off. See [T240_COMPACT_REQUEST_CANCELLATION.md](T240_COMPACT_REQUEST_CANCELLATION.md).
- [x] T241 Role-based authentication and per-space authorization; `hatPeer.CompactPeerRoleAuthorizer` adds default-deny command/space rules after the mandatory connection authorization, with an opt-in request callback and no wire-format change. See [T241_ROLE_BASED_AUTHORIZATION.md](T241_ROLE_BASED_AUTHORIZATION.md) and [BENCHMARK.md](BENCHMARK.md#t241-role-based-and-per-space-authorization).
- [x] T242 Append-only audit logging for administrative and data operations; opt-in complete monitoring HTTP coverage with detailed-event de-duplication. See [T242_APPEND_ONLY_AUDIT.md](T242_APPEND_ONLY_AUDIT.md) and [BENCHMARK.md](BENCHMARK.md#t242-append-only-audit-coverage).
- [x] T243 Mutual TLS authentication with certificate rotation; see [T243_MTLS_CERTIFICATE_ROTATION.md](T243_MTLS_CERTIFICATE_ROTATION.md).
- [x] T244 Queue task delay and deadline scheduling; `hatDataStructure.DelayQueue` and delayed `EnqueueAt`/`EnqueueAfter` operations provide stable deadline ordering with allocation-free ready pops. See [DELAY_QUEUE.md](DELAY_QUEUE.md), [VISIBILITY_QUEUE.md](VISIBILITY_QUEUE.md), and [BENCHMARK.md](BENCHMARK.md#delay-queue).
- [x] T245 Queue visibility timeouts for worker crash recovery; `VisibilityQueue` and `PriorityVisibilityQueue` hide leased work until acknowledgement, nack, or expiry, with epoch-fenced tokens for restart safety. See [VISIBILITY_QUEUE.md](VISIBILITY_QUEUE.md), [PRIORITY_VISIBILITY_QUEUE.md](PRIORITY_VISIBILITY_QUEUE.md), and [BENCHMARK.md](BENCHMARK.md#visibility-timeout-queue).
- [x] T246 Priority queues with starvation bounds. Opt-in `PriorityVisibilityQueueOptions.StarvationAfter` guarantees a ready item is not bypassed more than the configured number of leases; the default remains strict priority. See [T246_PRIORITY_QUEUE_STARVATION.md](T246_PRIORITY_QUEUE_STARVATION.md) and [BENCHMARK.md](BENCHMARK.md#t246-priority-queue-starvation-bounds).
- [x] T247 Queue task deduplication by client-supplied identity. `DeduplicatingPriorityVisibilityQueue[T]` rejects duplicate pending/leased keys, preserves identity through retry, expiry, snapshots, and atomic file checkpoints, and keeps the base queue unchanged; see [T247_DEDUPLICATING_QUEUE.md](T247_DEDUPLICATING_QUEUE.md) and [BENCHMARK.md](BENCHMARK.md#t247-deduplicating-priority-visibility-queue).
- [x] T248 Retry counters and dead-letter routing for failed tasks. `RetryingDeduplicatingPriorityVisibilityQueue[T]` applies max-attempt policy to explicit and expired leases, preserves FIFO dead letters through CRC-protected atomic snapshots, and keeps the base queue unchanged; see [T248_RETRY_DEAD_LETTER.md](T248_RETRY_DEAD_LETTER.md) and [BENCHMARK.md](BENCHMARK.md#t248-retry-counters-and-dead-letter-routing).
- [x] T249 Queue capacity, age, retry, and consumer-lag metrics. `EnableMetrics` is off by default, reports capacity/age/consumer lag/retry/dead-letter state, and preserves the opt-in flag through priority queue snapshots; see [T249_QUEUE_METRICS.md](T249_QUEUE_METRICS.md) and [BENCHMARK.md](BENCHMARK.md#t249-queue-capacity-age-retry-and-consumer-lag-metrics).
- [x] T250 Gap-safe sequence allocation with durable current value. `hatDataStructure.DurableSequence` provides atomic in-memory allocation plus a checksummed two-slot durable file with corruption fallback; see [T250_DURABLE_SEQUENCE.md](T250_DURABLE_SEQUENCE.md) and [BENCHMARK.md](BENCHMARK.md#t250-gap-safe-sequence-allocation-with-durable-current-value).
## C204 Status

C204 is adopted. Idempotency keys now propagate from async journal entries
through incremental projections into refreshed materialized-view status. See
[C204_PROJECTION_IDEMPOTENCY.md](C204_PROJECTION_IDEMPOTENCY.md) and the
paired measurements in [BENCHMARK.md](BENCHMARK.md#c204-projection-idempotency-metadata).
