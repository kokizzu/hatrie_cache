# Product Inspiration Checklist

This is a working inventory of ideas that may be useful to `hatrie_cache` after
comparing its current behavior with ClickHouse, Materialize, and Tarantool. It
is intentionally broad: an unchecked item is a research candidate, not a
promise to implement it.

## How To Read This

- `[x]` means the repository already has the capability, or a compatible local
  equivalent, and the adoption matrix is the source of detail.
- `[ ]` means it has not been adopted or has not yet been verified.
- `[-]` means it is deliberately deferred because its semantics or operational
  cost do not fit the current product.
- Every implementation must have a focused regression test before the change,
  a focused test after the change, a broader verification run, and a benchmark
  when performance is the reason for adopting it.
- Existing coverage is summarized in
  [ADOPTED_QUERY_ENGINE_IDEAS.md](ADOPTED_QUERY_ENGINE_IDEAS.md). This file is
  the larger research backlog; it is not a second implementation matrix.

## First Implemented Item

`C073` was the first item selected for implementation in this goal:

- ClickHouse-style two-level aggregation for high-cardinality columnar GROUP BY
  queries when the caller explicitly enables more than one worker;
- each worker builds a local aggregate state, then states are merged in first
  input order so existing deterministic output remains intact;
- the default sequential path remains unchanged when `Workers` is zero or one;
- group limits, NULL handling, numeric aggregate semantics, cancellation, and
  output byte limits must be preserved;
- CPU time, allocations, retained memory, and result ordering must be measured
  against the existing one-map path; the change is reverted if it regresses the
  intended high-cardinality workload without an acceptable reason.

The inspiration is ClickHouse query-result caching and its broader separation of
query planning, execution, and reusable read state. The implementation will use
the repository's existing version and snapshot contracts rather than weakening
them. Relevant official material:

- [ClickHouse query optimization guide](https://clickhouse.com/resources/engineering/clickhouse-query-optimisation-definitive-guide)
- [ClickHouse vectorized query execution](https://clickhouse.com/resources/engineering/vectorized-query-execution)
- [Materialize arrangements](https://materialize.com/docs/get-started/arrangements/)
- [Materialize EXPLAIN plans](https://materialize.com/docs/sql/explain-plan/)
- [Tarantool indexes](https://www.tarantool.io/en/doc/latest/platform/ddl_dml/indexes/)
- [Tarantool replication information](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_info/replication/)

## ClickHouse Ideas

The ClickHouse list is grouped by storage, execution, indexing, ingestion,
operations, and distributed behavior. The checklist is deliberately specific so
future work can be measured as a concrete behavior rather than as a product
name.

### Storage And Layout

- [x] C001 Column-oriented storage for analytical values - local columnar tables cover the compatible case.
- [x] C002 Read only referenced columns - SQL projection pruning is present.
- [x] C003 Granule-sized reads - columnar blocks provide the current scan unit.
- [x] C004 Primary ordering chosen from common predicates. `SQLIndexAdvisor.PrimaryOrderRecommendations` groups existing bounded slow-scan field counts into deterministic source-local field orders; it is advisory-only and leaves the default query planner and storage layout unchanged.
- [x] C005 Sparse primary-key index with mark pruning. Typed-table columnar caches can opt in to binary-searching validated nondecreasing numeric segment bounds; unordered, NULL, or NaN data retains the ordinary scan.
- [x] C006 Min/max skipping over ordered blocks.
- [x] C007 Set-style skipping for low-cardinality predicates.
- [x] C008 Bloom-filter skipping for equality and membership predicates.
- [x] C009 Token Bloom filters for word-oriented search - public allocation-free Unicode token sidecars reuse the compact Bloom snapshot format; [TOKEN_BLOOM_FILTER.md](TOKEN_BLOOM_FILTER.md).
- [x] C010 N-gram skipping for substring-oriented search.
- [x] C011 PREWHERE-style early filtering.
- [x] C012 Late materialization after filtering.
- [x] C013 Vectorized block execution.
- [x] C014 Selection vectors for filtered blocks.
- [x] C015 SIMD kernels for common numeric and string predicates. AVX2-gated equality and inequality dispatch is available for typed int64 masks; all other numeric and string predicates retain the portable allocation-free path.
- [x] C015a Allocation-free batch predicate masks for numeric and string filters.
- [x] C016 Pipeline stages with independently scheduled work. Bounded queues, independent worker pools, backpressure, cancellation, and stage-scoped errors are provided by hatPipeline (see PIPELINE_STAGES.md).
- [x] C017 EXPLAIN PIPELINE output with stage and worker detail. `EXPLAIN PIPELINE` now emits additive one-based stage, worker, and worker-count metadata derived from the static plan; regular `EXPLAIN` output remains unchanged and `EXPLAIN PIPELINE ANALYZE` is rejected explicitly.
- [x] C018 EXPLAIN output for the local SQL plan.
- [x] C019 Version-aware bounded query-result cache - `SQLQueryOptions.ResultCache` now wires the typed version-validated path into eligible materialized SQL queries; nil remains the default and unversioned or unsafe queries bypass retention. See [BENCHMARK.md](BENCHMARK.md#sql-result-cache).
- [x] C020 Query-condition cache for reusable predicate state.
- [x] C021 Explicit projections for reusable sorted or aggregated data.
- [x] C022 Automatic selection of compatible projections.
- [x] C023 Materialized projection maintenance.
- [x] C024 Refreshable materialized views with an explicit refresh policy. `MaterializedViews.RefreshChanged`, `ManagedRefreshScheduler.AddMaterializedView`, and the disabled-by-default `IncrementalProjectionRunner` provide dependency-scoped refresh, fixed intervals, source-version checks, journal coalescing, and durable checkpoints. Verified with `make test-sql-refresh-scheduler`, `make test-sql-incremental-projection`, and `make benchmark-sql-incremental-projection`.
- [x] C025 Projection consistency checks against source versions.
- [x] C026 Independent persistent data parts through the Pebble-backed path.
- [x] C027 Part metadata and checksums.
- [x] C028 Background merge and compaction.
- [x] C029 Vertical merge that reads only changed columns. `MergeColumnarParts` requests fields independently from each part and avoids loading unrequested wide columns; see [COLUMNAR_VERTICAL_MERGE.md](COLUMNAR_VERTICAL_MERGE.md).
- [x] C030 Compact-part format selected by row count and width. `SelectSQLColumnarPartFormat` uses inclusive row and encoded-byte thresholds with conservative defaults and zero allocations; see [COLUMNAR_PART_FORMAT.md](COLUMNAR_PART_FORMAT.md).
- [x] C031 ReplacingMergeTree-style latest-row replacement. `ReplaceSQLRows` provides stable-key-order latest-version selection with deterministic tie handling and input isolation; see [REPLACING_MERGE.md](REPLACING_MERGE.md).
- [x] C031a Explicit stable-order replacing merge for versioned rows.
- [x] C032 CollapsingMergeTree-style sign-based row cancellation. `CollapseSQLRows` pairs unmatched opposite signs by logical key, preserves survivors in input order, and reports invalid signs; see [COLLAPSING_MERGE.md](COLLAPSING_MERGE.md).
- [x] C032a Explicit deterministic signed-row cancellation merge.
- [x] C033 SummingMergeTree-style merge-time summation. `SumSQLRows` combines explicitly selected numeric columns with stable key order, type checks, and overflow protection; see [SUMMING_MERGE.md](SUMMING_MERGE.md).
- [x] C033a Explicit overflow-checked summing merge for selected numeric columns.
- [x] C034 Aggregating states for reusable grouped results.
- [x] C035 Lightweight delete patch parts.
- [x] C036 Durable mutation queue with observable progress. `CommandJournal` and the LevelDB-backed `ReplicationOutboxStore` provide durable FIFO mutation recovery, dead-letter handling, bounded restore, and binary/JSON compatibility; `ReplayWithProgress` exposes concurrent replay progress and ETA. Verified with `make test-c036-durable-mutation-queue`, `make test-replay-progress`, `make test-race-replay-progress`, and `make benchmark-replay-progress`.
- [x] C037 TTL expiration for supported values and records.
- [x] C038 TTL-driven rollup or recompression. `TimeBucketRollup` supports verified bucket retention and boundary-only expiration without silently discarding partial buckets; see [TTL_ROLLUP.md](TTL_ROLLUP.md).
- [x] C039 Partition pruning for local partitions.
- [x] C040 Sampling key with deterministic SAMPLE semantics across partitions. `SampleSQLRows` hashes a caller-selected logical key with a seed, preserving selection across order and partition boundaries; see [DETERMINISTIC_SAMPLE.md](DETERMINISTIC_SAMPLE.md).
- [x] C040a Deterministic key-hash sampling across partition boundaries.
- [x] C041 Multiple disk policies with placement rules. `DiskPlacementPolicy` provides immutable weighted rules with deterministic key selection; see [DISK_PLACEMENT.md](DISK_PLACEMENT.md).
- [x] C041a Immutable weighted deterministic disk placement policy with duplicate/overflow validation.
- [x] C042 Hot, warm, and cold storage tiers. `StorageTierPolicy` selects an immutable age-threshold tier and delegates path placement; see [STORAGE_TIERS.md](STORAGE_TIERS.md).
- [x] C042a Immutable age-threshold hot, warm, and cold tier policy over disk placement.
- [x] C043 Remote object-store parts with local metadata. `RemotePartReference` validates supported object URIs and root-confined metadata paths; see [REMOTE_PARTS.md](REMOTE_PARTS.md).
- [x] C043a Validated immutable remote-part references with root-confined local metadata paths.
- [x] C044 Zero-copy replication of immutable parts. `hatMerkle.CopyImmutablePartFile` validates the declared file boundary and preserves `io.Copy` native transfer hooks; `CopyImmutablePart` supplies the verified streaming fallback. See [ZERO_COPY_PARTS.md](ZERO_COPY_PARTS.md).
- [x] C045 Part-level cache admission and eviction policy. `PartCachePolicy` provides explicit admission and deterministic LFU/LRU eviction planning; see [PART_CACHE_POLICY.md](PART_CACHE_POLICY.md).
- [x] C045a Explicit part-cache admission and deterministic LFU/LRU eviction planning.
- [x] C046 Read amplification accounting per part and column. `ReadAmplificationRegistry` and RowBinary read statistics aggregate deterministic per-part/per-column bytes and ratios; see [READ_AMPLIFICATION.md](READ_AMPLIFICATION.md).
- [x] C046a Per-part and per-column read amplification accounting with deterministic snapshots.
- [x] C046a Statistics-only per-column RowBinary read accounting.
- [x] C047 Adaptive granule sizing from observed predicate selectivity. `GranuleSizingPolicy` adapts bounded future scan granules from observed selectivity without retaining state or changing result semantics; see [GRANULE_SIZING.md](GRANULE_SIZING.md).
- [x] C047a Bounded adaptive granule sizing from observed predicate selectivity.
- [x] C047a Conservative block min/max predicate pruning.
- [x] C048 Compact numeric encodings selected from data statistics. Adaptive RowBinary evaluates legacy, delta, and double-delta encodings from full-batch or sampled statistics and records the selected codec in the header; see [ROW_BINARY_ADAPTIVE.md](ROW_BINARY_ADAPTIVE.md).
- [x] C048a Minimum-width bit-packed uint64 codec selected from column maximum with canonical validation.
- [x] C048a Exact full-batch codec selection by encoded size.
- [x] C049 Low-cardinality dictionary encoding for typed string values.
- [x] C050 Shared JSON subcolumns for repeated paths. `JSONSubcolumnRegistry` interns normalized JSON paths into process-local `uint32` IDs with concurrent lookup and snapshots; see [JSON_SUBCOLUMNS.md](JSON_SUBCOLUMNS.md).
- [x] C050a Process-local shared JSON subcolumn path interning with deterministic snapshots.
- [x] C051 JSON path indexing for supported predicates.
- [x] C052 Native array and nested-column physical layout. `hatSql.ColumnarListColumn` and `ColumnarNestedColumn` use shared offsets with flat child vectors and are integrated into columnar access, merge loading, and cache cloning. See [COLUMNAR_NESTED_LAYOUT.md](COLUMNAR_NESTED_LAYOUT.md).
- [x] C053 Bitmap-backed nullable columns. Nullable RowBinary encodes one bitmap per row, preserves typed values, and rejects malformed or unsafe input; see [ROW_BINARY_NULLABLE_BITMAP.md](ROW_BINARY_NULLABLE_BITMAP.md).
- [x] C053a One-bit-per-row nullable bitmap with resize preservation and population counting.
- [x] C053a Nullable-column bitmap RowBinary format.
- [x] C054 Fixed-width date and datetime encodings - RowBinary stores dates as 4-byte epoch days and datetimes as 8-byte Unix nanoseconds, with explicit round-trip and payload-size verification (see ROW_BINARY.md).
- [x] C055 Delta encoding for monotonically changing numeric columns.
- [x] C056 Double-delta encoding for timestamps.
- [x] C057 Gorilla-style floating-point encoding. Bit-preserving XOR window encoding and exact-input validation are provided by hatCodec (see GORILLA_FLOAT.md).
- [x] C057a Bit-preserving XOR window codec for repeated and slowly changing float64 values.
- [x] C058 Configurable storage compression codecs.
- [x] C059 Codec selection from sampled column entropy (see CODEC_SELECTION.md).
- [x] C059a Stackless byte-entropy estimator with conservative raw-or-compressed codec recommendation.
- [x] C059a Sampled adaptive codec selection from a bounded prefix.
- [x] C060 Compression ratio and decompression CPU accounting (see CODEC_METRICS.md).
- [x] C060a Atomic codec byte and CPU accounting with derived compression ratio.
- [x] C060a Opt-in codec size and synchronous decode-time accounting.

### Query Execution And SQL

- [x] C061 Parse, analyze, optimize, execute, and format as distinct phases.
- [x] C062 Predicate pushdown into source scans.
- [x] C063 Projection pruning through joins and aggregates.
- [x] C064 Constant and literal normalization for reusable plans.
- [x] C065 Constant folding across all scalar expressions - deterministic row-independent CAST, scalar functions, CASE, IN, BETWEEN, IS NULL, and REGEXP expressions are folded after parameter binding; unsupported/custom/aggregate expressions retain the established path. See [CONSTANT_FOLDING.md](CONSTANT_FOLDING.md).
- [x] C066 Predicate reordering by estimated cost and selectivity. Direct numeric columnar conjunctions reorder up to eight predicates using available segment min/max selectivity estimates; the original order remains unchanged without statistics or for larger conjunctions.
- [x] C067 Common-subexpression elimination. The execution rewrite removes exact duplicate deterministic pure subexpressions in boolean `A AND A` and `A OR A` predicates, while custom functions, subqueries, windows, filtered aggregates, and other query-dependent expressions retain the original tree.
- [x] C068 Short-circuit evaluation for expensive predicates. The columnar batch evaluator now skips deterministic, total right-hand `AND`/`OR` expressions for rows whose left value already determines the three-valued result; expressions that may raise errors retain eager evaluation.
- [x] C069 Expression indexes for supported scalar expressions.
- [x] C070 Dynamic JSON skip metadata.
- [x] C071 Hash aggregation.
- [x] C072 Compact hash aggregation for small grouped states.
- [x] C073 Two-level aggregation for high-cardinality grouped states - adopted explicitly for columnar inputs with `Workers >= 2`, at least two merge-safe `COUNT`/`MIN`/`MAX` states, and a 16,384-row threshold; benchmark and fallback rationale are in [SQL_TWO_LEVEL_AGGREGATION.md](SQL_TWO_LEVEL_AGGREGATION.md).
- [x] C074 External aggregation with a memory budget and spill path.
- [x] C075 Ordered aggregation when input ordering makes it cheaper.
- [x] C076 Approximate sketches for supported distinct and quantile queries.
- [x] C077 Aggregate combinators for reusable state, merge, and finalize phases. `SQLAggregateState` and its concurrent registry separate worker-local `Add`, partial `Merge`, and final `Finalize` phases; see [AGGREGATE_COMBINATORS.md](AGGREGATE_COMBINATORS.md).
- [x] C077a Reusable aggregate state, merge, and finalize combinator registry.
- [x] C078 Top-K aggregation.
- [x] C079 Dynamic Top-N skipping.
- [x] C080 LIMIT BY per-group limiting.
- [x] C081 WITH FILL and gap filling for ordered time series. SQL fills bounded timestamp gaps after ordered projection, honors result limits and aliases, and supports streaming output; see [WITH_FILL.md](WITH_FILL.md).
- [x] C081a Ordered time-series gap filling with explicit half-open bounds.
- [x] C081b SQL WITH FILL grammar for bounded TIMESTAMP/DURATION ordered series.
- [x] C082 Window functions for the supported SQL subset.
- [x] C083a ArrayJoin-style row expansion for array and slice values.
- [x] C084a Array and map aggregate functions with deterministic NULL and duplicate-key semantics.
- [x] C085 Hash joins.
- [x] C086 Grace-hash or spillable joins.
- [x] C087 Runtime Bloom filters for joins.
- [x] C088 Partial-merge joins for sorted sources via `hatSql.MergeSortedTypedTableJoin`, with callback streaming, duplicate-run handling, SQL NULL/NaN semantics, input validation, and benchmark guide.
- [x] C089 Direct lookup joins through indexed sources.
- [x] C090 Join reordering for compatible query shapes.
- [x] C091 Distributed joins with explicit data movement accounting.
- [x] C092 Semi-join and anti-join execution where supported.
- [x] C093 Distributed partial aggregation followed by merge.
- [x] C094 External sorting with bounded memory.
- [x] C095 Sort and spill safety limits.
- [x] C096 LIMIT and Top-N early termination.
- [x] C097 Query cancellation through context propagation.
- [x] C098 Query memory budgets and allocation reporting.
- [x] C099 Query timeout and deadline enforcement.
- [x] C100 Query quotas by user, tenant, or source - NamespaceQueryGovernor supports per-namespace fixed-window request quotas with default-off behavior and per-namespace tightening (see QUERY_GOVERNANCE.md).
- [x] C101 Query result cache with explicit freshness and invalidation policy. `ResultCache.Execute` requires an epoch callback, serves hits only for the same epoch, invalidates stale entries, and returns independent result snapshots; existing tests cover freshness changes and nested plan/result isolation.
- [x] C102 Prepared-plan cache keyed by normalized SQL and schema version. `SQLPreparedQueryCache` canonicalizes lexer tokens while preserving identifiers and literal values, namespaces entries with `PreparedSchemaVersion`, keeps an allocation-free exact-source fast path, and exposes `PrepareSQLQueryWithSchemaVersion`; focused execution tests and cache benchmarks cover normalization, literal safety, version isolation, and LRU behavior.
- [x] C103 Plan invalidation when an index or projection changes. `SQLPreparedQueryCache.InvalidateSchemaVersion` removes only the affected version namespace and `Invalidate` clears all parsed plans and source aliases while retaining capacity; rebuild callers can release stale plans explicitly without adding work to query hits, and tests cover version isolation, alias cleanup, and refill behavior.
- [x] C104 Query result reuse across equivalent parameter bindings. `NewSQLResultCache` reuses equivalent source/parameter bindings under the trie mutation epoch, separates different bindings, and returns independent result snapshots; focused tests cover both parameter isolation and hit cloning, with the portable hit benchmark retained.
- [x] C105 Query fingerprinting independent of literal values. `SQLQueryFingerprint` validates the shared SQL grammar, preserves identifiers/operators/literal types/parameter positions, replaces literal values with type markers, and returns a SHA-256 digest without retaining query text.
- [x] C106 Workload classes with separate concurrency and memory budgets. `NamespaceQueryGovernor` applies per-namespace concurrency, queue, rate, row, operator-byte, worker, spill, recursion, and timeout ceilings before execution; see [SQL_WORKLOAD_CLASSES.md](SQL_WORKLOAD_CLASSES.md).
- [x] C107 Admission control before expensive scans. `NamespaceQueryGovernor` bounds and queues namespaces before starting SQL execution, honors cancellation, applies quotas, and leaves scan-time budgets in force; see [SQL_ADMISSION_CONTROL.md](SQL_ADMISSION_CONTROL.md).
- [x] C107a Bounded query-admission queues - Cap namespace waiters before allocation while preserving the default unlimited behavior (see QUERY_GOVERNANCE.md).
- [x] C108 Kill-query command with an operator-visible reason. `SQLQueryManager.Cancel` provides bounded, reasoned cooperative cancellation; remote layers must apply their existing auth policy.
- [x] C109 Per-operator CPU and row counters. `SQLQueryEvent.Operators` reports privacy-safe input/output rows, bytes, and elapsed nanoseconds for each observed operator; existing observer tests cover the counters.
- [x] C110 Query pipeline trace export. `NewQueryTraceRecorder` provides an opt-in bounded observer with independent snapshots and JSONL export, retaining only the newest events when a positive limit is configured; focused tests cover bounds, cloning, and writer errors.

### Ingestion, Formats, And Operations

- [x] C111 Asynchronous inserts with bounded queues.
- [x] C112 Wait-for-async-insert acknowledgement mode.
- [x] C113 Idempotent insert deduplication.
- [x] C114 Adaptive async-insert batching.
- [x] C115 Parallel input parsing with deterministic error reporting. `ParseNDJSONParallel` parses independent records with contiguous worker ranges, preserves row order, and returns the lowest invalid source line; `ImportNDJSONParallel` commits only after complete validation. See [PARALLEL_INPUT.md](PARALLEL_INPUT.md).
- [x] C116 Binary wire format negotiation.
- [x] C117 RowBinary-compatible encoding. `EncodeSQLRowBinary` and `DecodeSQLRowBinary` provide an explicit schema-aware RowBinary-style stream with little-endian fixed-width values, varint-length strings/bytes, nullable markers, strict validation, and bounded decoding; JSON/protobuf defaults remain unchanged. See [SQL_ROW_BINARY.md](SQL_ROW_BINARY.md).
- [x] C118 JSON row encoding for compatibility paths.
- [x] C119 Arrow-compatible column transfer.
- [x] C120 Parquet import and export. `ExternalTables` provides `ExportParquet`, `ImportParquet`, and `WriteParquet` over the existing in-memory external-table registry; round-trip tests and `BenchmarkExternalTablesExportTransfer` cover the path.
- [x] C121 Native compressed block transfer with independent blocks. `hatCodec.EncodeCompressedBlocks` and `DecodeCompressedBlocks` provide an opt-in `HCB1` stream with independently bounded raw/DEFLATE blocks, raw fallback, and CRC32 validation; JSON/protobuf/gzip defaults remain unchanged. The measured tradeoff is faster encoding but larger wire output and slower decoding than one gzip stream. See [COMPRESSED_BLOCKS.md](COMPRESSED_BLOCKS.md).
- [x] C122 Configurable wire compression.
- [x] C123 Compression level negotiation per client (see COMPRESSION_NEGOTIATION.md).
- [x] C123a Compatible per-client compression level negotiation with explicit range intersection.
- [x] C124 Column statistics in wire metadata. `BuildSQLRowBinaryColumnStats`, `EncodeSQLRowBinaryWithStats`, and `DecodeSQLRowBinaryWithStats` provide exact NULL/value counts and typed min/max metadata in an opt-in `HBS1` envelope; decoding recomputes and validates the metadata before returning it, while plain RowBinary remains unchanged. See [ROW_BINARY_STATS.md](ROW_BINARY_STATS.md).
- [x] C125 Dictionary transfer reuse across batches. `NewSQLRowBinaryDictionaryEncoder` and `NewSQLRowBinaryDictionaryDecoder` retain selected string/bytes/JSON dictionaries across schema-compatible batches, send only additions plus compact ids, bound retained state, and leave plain RowBinary unchanged. Benchmarks cover first/reused encode/decode paths and show the bandwidth/CPU tradeoff. See [ROW_BINARY_DICTIONARY.md](ROW_BINARY_DICTIONARY.md).
- [x] C126 Backup snapshots with checksums.
- [x] C127 Incremental journal-backed backup.
- [x] C128 Object-store backup targets (see OBJECT_STORE_BACKUP.md).
- [x] C128a Streaming object-store backup targets with verified manifests and atomic restore.
- [x] C129 Restore verification for supported data types.
- [x] C130 Recovery checkpoints and replay validation.
- [x] C131 Metrics and health endpoints are opt-in.
- [x] C132 System tables for parts, mutations, and query history. `SQLSystemTablesResolver` exposes bounded `system.parts`, `system.mutations`, `system.queries`, and `system.query_history` views without mutation values or query text; see [SYSTEM_TABLES.md](SYSTEM_TABLES.md).
- [x] C133 Query log retention and sampling policy - SQLQueryManager provides bounded privacy-safe history with deterministic configurable completion sampling and no SQL-text retention (see QUERY_HISTORY.md).
- [x] C134 OpenTelemetry spans for query phases. `QueryTraceRecorder.OpenTelemetrySpans` exposes SDK-neutral root/query and operator child spans with OTLP-width IDs, status, counters, cloned attributes, and documented end-anchored phase timing. It is opt-in and keeps SQL/error/row data out of exported spans; see [QUERY_TRACING.md](QUERY_TRACING.md).
- [x] C135 Trace IDs carried through remote work. `hat/hatTrace` validates W3C
  `traceparent` values and carries them through monitoring HTTP extraction,
  HTTP replication headers, inbound gRPC metadata, and gRPC replication
  metadata without generating IDs or changing no-header behavior. Covered by
  focused normal and race tests.
- [x] C136 Per-tenant resource quotas - NamespaceQueryGovernor applies immutable, tightening per-namespace resource policies suitable for tenant, user, or source isolation (see TENANT_RESOURCE_LIMITS.md).
- [x] C137 TLS, authentication, and authorization controls.
- [x] C138 Operational config validation with sane defaults.
- [x] C139 Regression and compatibility test matrix.
- [x] C140 Benchmark reports with raw results and memory measurements.

### Distributed And Replicated Behavior

- [x] C141 Distributed table abstraction over partitions. `hatSql.PartitionedSourceResolver` exposes ordered physical row partitions as one logical `CACHE`/`KEYS` source with immutable zero-copy query admission and explicit fallback to the existing resolver contract; see [PARTITIONED_SQL_SOURCES.md](PARTITIONED_SQL_SOURCES.md).
- [x] C142 Automatic shard pruning from partition predicates. `hatSql.PartitionPruningSourceResolver` prunes complete physical partition subsets for literal equality and `IN` conjuncts while re-evaluating the full `WHERE`; unsupported or unavailable predicates preserve the existing scan path. See [PARTITION_PRUNING.md](PARTITION_PRUNING.md).
- [x] C143 Parallel replicas for one query. Added opt-in first-success `hatReplication.ExecuteParallelReplicaRead` fan-out with bounded named replicas, cancellation of losing callbacks, and input-order attempt reporting; see [PARALLEL_REPLICA_READS.md](PARALLEL_REPLICA_READS.md).
- [x] C144 Hedged reads for slow replicas. The same coordinator supports zero-delay parallel fan-out or positive-delay hedges, with explicit timer/callback cost and no default routing change; see [PARALLEL_REPLICA_READS.md](PARALLEL_REPLICA_READS.md).
- [x] C145 Remote read retries with bounded duplicate work - `hatSql.ReadReplicaSet.ExecuteWithRetry` rotates across replicas with an explicit retry classifier, context-aware backoff, default-off behavior, and a hard eight-attempt cap; see [READ_REPLICA_RETRIES.md](READ_REPLICA_RETRIES.md).
- [x] C146 Distributed partial aggregation and final merge - `hatSql.TypedTableAggregate.MergePartial` and `MergePartials` combine exact partition-local COUNT, SUM, MIN, MAX, and COUNT DISTINCT state with deterministic validation and a measured replay comparison; see [DISTRIBUTED_PARTIAL_AGGREGATION.md](DISTRIBUTED_PARTIAL_AGGREGATION.md).
- [x] C147 Quorum inserts with explicit durability policy - `hatReplication.ExecuteWriteQuorum` runs named replica writes concurrently against an explicit acknowledgement threshold, reports deterministic per-target outcomes, and remains opt-in; callers retain responsibility for reconciliation and idempotency after partial failure; see [WRITE_QUORUM.md](WRITE_QUORUM.md).
- [x] C148 Replicated part exchange with checksums - `hatMerkle` provides exact-size plus SHA-256 `PartChecksum` metadata, canonical transfer encoding, streaming immutable-part copy, and corruption/size validation; see [PART_CHECKSUMS.md](PART_CHECKSUMS.md).
- [x] C148a Immutable-part length and SHA-256 checksums.
- [x] C149 Replication queue introspection. `ReplicationResult.Queue` exposes `QueueStats` with depth, capacity, attempts, acknowledgements, failures, dead letters, pause state, and vector-clock state; monitoring also exports the queue health metrics.
- [x] C150 Replica lag thresholds for read routing. `ReadReplicaPolicy` enforces required frontiers and maximum lag before deterministic candidate selection; see [REPLICA_LAG_ROUTING.md](REPLICA_LAG_ROUTING.md).
- [x] C151 Read-after-write consistency levels. Implemented in
  `hat/hatReplication` with eventual, bounded-staleness, and read-after-write
  replica selection while preserving the legacy selector default.
- [x] C152 Leader election independent from query workers (see LEADER_ELECTION.md).
- [ ] C153 Metadata consensus for partition ownership.
- [x] C153a Consensus-bound, retry-safe topology commit admission with
  fingerprint compare-and-swap, strict-majority vote evaluation, and fencing
  monotonicity; transport and vote authentication remain caller-owned. See
  [TOPOLOGY_CONSENSUS.md](TOPOLOGY_CONSENSUS.md).
- [x] C153b Partition-ownership metadata quorum validation binding shard,
  primary, replica order, topology fingerprint, and fencing token; transport
  and vote authentication remain caller-owned. See
  [PARTITION_OWNERSHIP_CONSENSUS.md](PARTITION_OWNERSHIP_CONSENSUS.md).
- [ ] C154 Rolling schema changes across replicas.
- [x] C154a Conservative rolling-schema compatibility preflight over validated
  schemas; exact replication fingerprint enforcement remains unchanged. See
  [SCHEMA_COMPATIBILITY.md](SCHEMA_COMPATIBILITY.md).
- [x] C154b Explicit validated schema-history policy wired into HTTP and gRPC
  replication gates; unknown contracts remain rejected and nil preserves exact
  matching. See [SCHEMA_COMPATIBILITY.md](SCHEMA_COMPATIBILITY.md).
- [x] C154c Stateful, retry-safe rolling deployment phases for validated replica
  schema transitions; network installation and activation remain caller-owned.
  See [SCHEMA_ROLLOUT.md](SCHEMA_ROLLOUT.md).
- [x] C154d Sequential retry-safe rolling-schema coordinator with deterministic
  node ordering, independent hook snapshots, cancellation checkpoints, and
  phase-preserving retries; transport and authentication remain caller-owned.
  See [SCHEMA_ROLLOUT.md](SCHEMA_ROLLOUT.md).
- [x] C155 Rolling binary upgrades with compatibility gates for the gRPC and HTTP command protocols; schema compatibility remains tracked separately under C154.
- [x] C155a gRPC protocol-version metadata negotiation and server compatibility gates with legacy omission defaults; see [GRPC_PROTOCOL_COMPATIBILITY.md](GRPC_PROTOCOL_COMPATIBILITY.md).
- [x] C155b Configurable HTTP command protocol ranges and optional HTTP replication-client range advertisement with legacy omission defaults; see [HTTP_PROTOCOL_COMPATIBILITY.md](HTTP_PROTOCOL_COMPATIBILITY.md).
- [x] C156 Cross-region replication policy (see CROSS_REGION_REPLICATION.md).
- [x] C157 Cross-region backup restore drill. The object-store verification drill checks transferred payload sizes and SHA-256 digests before restore and exercises tamper rejection; see [CROSS_REGION_RESTORE_DRILL.md](CROSS_REGION_RESTORE_DRILL.md).
- [x] C157a Cross-region backup/restore integrity drill (see CROSS_REGION_RESTORE_DRILL.md).
- [x] C158 Split-brain fencing (see SPLIT_BRAIN_FENCING.md).
- [x] C159 Failure-domain-aware replica placement (see FAILURE_DOMAIN_PLACEMENT.md).
- [x] C160 Query routing by region and locality. `hatReplication.SelectReadReplicaWithConsistency` accepts optional ordered preferred regions, applies them only after consistency filtering, and falls back to any eligible region; the zero-value policy preserves the prior freshness/health/name ordering. See [REPLICA_LOCALITY_ROUTING.md](REPLICA_LOCALITY_ROUTING.md).
- [x] C161 ClickHouse-style metadata-only COUNT(*) for direct predicate-free columnar sources; the executor uses validated batch row counts without a row loop while richer or filtered aggregates retain the established path. See [BENCHMARK.md](BENCHMARK.md#columnar-metadata-count).
- [x] C162 ClickHouse-style metadata-only MIN/MAX for direct predicate-free numeric columnar sources; complete finite segment bounds are combined without a row loop, while incomplete or ambiguous metadata retains the established scan. See [BENCHMARK.md](BENCHMARK.md#columnar-metadata-minmax).
- [x] C163 ClickHouse-style dictionary membership shortcuts for filtered `COUNT(*)`; validated low-cardinality dictionaries answer absent equality, inequality, and `IN` predicates without decoding row codes, while matching, richer, untrusted, or invalid inputs retain the established scan and validation behavior. See [BENCHMARK.md](BENCHMARK.md#columnar-dictionary-membership-count).
- [x] C164 ClickHouse-style exact low-cardinality set marks per columnar segment; trusted typed-table dictionaries publish compact 64-bit membership masks so compatible equality, inequality, and literal `IN` scans skip impossible segments while all remaining rows retain normal validation. See [BENCHMARK.md](BENCHMARK.md#columnar-dictionary-segment-marks).
- [x] C165 ClickHouse-style range data skipping for general columnar filters. Ordinary field-only numeric `CACHE` scans now use valid per-segment min/max bounds to skip disjoint segments while retaining the existing row matcher and fallback behavior; see [COLUMNAR_RANGE_SKIPPING.md](COLUMNAR_RANGE_SKIPPING.md).

## Materialize Ideas

Materialize contributes ideas about incremental dataflow, arrangements, logical
time, and self-correcting results. Items marked adopted refer to local behavior
with equivalent safety properties; they do not claim implementation parity with
Materialize's Timely/Differential Dataflow runtime.

### Dataflow, Arrangements, And Incremental Results

- [x] M001 Incremental data-parallel dataflow for supported table paths.
- [x] M002 Generic `(data,time,diff)` multiset representation. `DifferentialMultiset[T]` stores comparable data and timestamps with signed diffs, immediate zero consolidation, and overflow-safe updates; see [DIFFERENTIAL_MULTISET.md](DIFFERENTIAL_MULTISET.md).
- [x] M002a Generic differential multiset keyed by comparable data and time with zero-consolidation and overflow checks.
- [x] M002a Exported differential row batch representation.
- [x] M003 Timely-style nested worker scopes. `hatPipeline.Scope` provides
  cancellable worker ownership, first-error propagation, deterministic waiting,
  and `GoChild` nesting without changing existing `Pipeline.Run` behavior; see
  [PIPELINE_SCOPES.md](PIPELINE_SCOPES.md).
- [x] M004 Data-parallel map, filter, project, and reduce stages where supported.
- [x] M005 Shared arrangements reused by multiple compatible queries.
- [x] M006 Arrangement keys derived from indexed predicates.
- [x] M007 Arrangement reuse across subscriptions and point reads.
- [x] M008 Arrangement compaction for old versions.
- [x] M009 Consolidation of equal data and opposite diffs. `ConsolidateDifferentialRows` combines equal key/time updates, preserves signed multiplicity, removes zero totals, and rejects overflow; see [DIFFERENTIAL_ROWS.md](DIFFERENTIAL_ROWS.md).
- [x] M009a Overflow-safe batch consolidation by key and time.
- [x] M010 Reduce/group arrangements for typed tables.
- [x] M011 Join arrangements for typed indexed tables.
- [x] M012 Distinct arrangements for typed sources.
- [x] M013 Top-K arrangements for ordered subscriptions.
- [x] M014 Lookup arrangements for external or remote sources. `LookupSourceResolver` provides optional equality lookups for `EXTERNAL(...)` sources with full-scan fallback and predicate re-evaluation. See [LOOKUP_ARRANGEMENTS.md](LOOKUP_ARRANGEMENTS.md).
- [x] M015 Delta joins that avoid repeated large-side scans. `DifferentialTemporalJoin` retains per-key rows and probes only matching counterpart groups for signed changes; see [DELTA_JOINS.md](DELTA_JOINS.md).
- [x] M016 Linear joins with explicit maintained indexes. `TypedTableJoin` maintains typed equality buckets and matched pairs across ordered inserts, updates, and deletes; see [LINEAR_INDEXED_JOINS.md](LINEAR_INDEXED_JOINS.md).
- [x] M017 Semijoin reduction before maintaining a join via opt-in typed-table join options, checkpoint-safe rehydration, demotion, retention stats, tests, and benchmark guide.
- [x] M018 Monotonicity analysis for cheaper maintenance. `TypedTableChangesAreMonotone` and `TypedTableAggregate.ApplyAuto` select the existing insert-only path only for proven append-only batches and fall back to general maintenance otherwise. See [MONOTONE_MAINTENANCE.md](MONOTONE_MAINTENANCE.md).
- [x] M019 Key derivation for supported SQL predicates.
- [x] M020 Key-aware planning and index selection.
- [x] M021 Explicit logical timestamps in snapshots and subscriptions.
- [x] M022 AS OF-style historical reads where the source supports them.
- [x] M023 UP TO-style bounded subscription reads.
- [x] M024 Since/read frontier tracking.
- [x] M025 Upper/write frontier tracking.
- [x] M026 Write progress tracking.
- [x] M027 Read progress tracking.
- [x] M028 Progress messages for streaming consumers.
- [x] M029 SUBSCRIBE-style streaming results.
- [x] M030 Snapshot-then-tail subscription startup.
- [x] M031 Transaction-consistent snapshots.
- [ ] M032 Strong consistency across all independent source partitions.
- [x] M032a Opt-in SQL snapshot provider pins every source read in one query to a caller-owned immutable view; full distributed frontier coordination remains open. See [SQL_SNAPSHOT_PROVIDER.md](SQL_SNAPSHOT_PROVIDER.md).
- [x] M032b Fixed-partition indexed common frontier tracking with O(1) readiness checks; physical source waiting remains caller-owned. See [SQL_SOURCE_FRONTIERS.md](SQL_SOURCE_FRONTIERS.md).
- [x] M032c Bounded context-aware waits over indexed common source frontiers, with atomic batch publication, generation-based wake-ups, and an allocation-free cached ready path; physical snapshot acquisition remains caller-owned. See [SQL_SOURCE_FRONTIERS.md](SQL_SOURCE_FRONTIERS.md#bounded-snapshot-barrier).
- [x] M032d Frontier-bound SQL snapshot provider contract. `BeginSQLFrontierSnapshot` waits for the common source frontier and requires an explicit `BeginSQLSnapshotAt` provider, rejecting silent fallback to an unbounded snapshot; physical source coordination remains provider-owned. See [SQL_FRONTIER_SNAPSHOTS.md](SQL_FRONTIER_SNAPSHOTS.md).
- [ ] M033 Timestamp oracle for globally ordered writes.
- [x] M033a Process-local lock-free Lamport timestamp allocation with monotone remote observation; see [TIMESTAMP_ORACLE.md](TIMESTAMP_ORACLE.md). Global cross-node ordering remains open.
- [x] M033b Consensus-bound global timestamp reservations with term and node-epoch fencing, idempotent per-node sequences, deterministic snapshots, and allocation-free range leases; transport and leader election remain caller-owned. See [GLOBAL_TIMESTAMP_ORACLE.md](GLOBAL_TIMESTAMP_ORACLE.md).
- [x] M034 Epoch management for restarts and leases. `hatStorage.PersistentShardLease` and `hatStorage.PersistentNodeEpoch` durably advance fencing tokens across restarts, reject concurrent owners, and expose explicit renew, release, inspect, and validation operations; see [PERSISTENT_NODE_EPOCHS.md](PERSISTENT_NODE_EPOCHS.md).
- [x] M034a Durable local node epochs with monotone restart fencing; see [PERSISTENT_NODE_EPOCHS.md](PERSISTENT_NODE_EPOCHS.md).
- [x] M035 Self-correcting materialized results for typed arrangements.
- [x] M036 Retractions and insertions on typed updates.
- [ ] M037 Generic negative-diff support for every SQL operator.
- [ ] M037e Generic keyed differential reduction was benchmarked and rolled back because the arbitrary callback path was about 7.97x slower, 6.36x larger in transient bytes, and 5.67x more allocation-heavy than the existing specialized reducer; see [BENCHMARK.md](BENCHMARK.md#rejected-generic-keyed-differential-reduction).
- [x] M037a Signed negative diffs in the reusable batch primitive.
- [x] M037b Generic signed filter, map, flat-map, union, and join operators with atomic callback failure handling; see [DIFFERENTIAL_OPERATORS.md](DIFFERENTIAL_OPERATORS.md).
- [x] M037c Signed differential int64 SUM maintenance for callback-defined groups with checked weighted updates; see [DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md).
- [x] M037d One-pass signed differential COUNT+SUM maintenance for callback-defined groups; see [DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md).
- [x] M037f Materialize-style signed differential `EXCEPT` and explicit
  weight negation with checked `math.MinInt64` handling, duplicate-preserving
  consolidation, and input ownership guarantees; see
  [DIFFERENTIAL_DIFFERENCE.md](DIFFERENTIAL_DIFFERENCE.md).
- [x] M037g Signed differential int64 grouped `MIN`/`MAX` maintenance with
  exact duplicate retractions and endpoint restoration; see
  [DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md).
- [ ] M038 Generic multiset duplicate preservation across all operators.
- [x] M038a Duplicate multiplicity retained as signed diff weights.
- [x] M038b Multiset-preserving expansion, union, and weighted join composition; see [DIFFERENTIAL_OPERATORS.md](DIFFERENTIAL_OPERATORS.md).
- [x] M038c Weighted duplicate-preserving differential SUM maintenance for callback-defined groups; see [DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md).
- [x] M038d One-pass weighted duplicate-preserving differential COUNT+SUM maintenance; see [DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md).
- [x] M038e Stateful duplicate-preserving differential `INTERSECT ALL`
  maintenance with checked multiplicities, atomic validation, and incremental
  output deltas; see [DIFFERENTIAL_INTERSECT.md](DIFFERENTIAL_INTERSECT.md).
- [x] M038g Weighted duplicate-preserving differential grouped `MIN`/`MAX`
  maintenance with exact value multiplicity tracking; see
  [DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md).
- [x] M038f SQL parser and executor support for duplicate-preserving
  `INTERSECT ALL` and `EXCEPT ALL`, including collation-aware multiplicity and
  left-order output; see [SQL_SET_OPERATIONS.md](SQL_SET_OPERATIONS.md).
- [x] M039 Compaction constrained by active read frontiers.
- [x] M040 Append-only fast path selected from source metadata via typed-table change metadata, validating aggregate dispatch, compatibility tests, and benchmark guide.
- [x] M041 Upsert-source semantics with key replacement. `TypedTable.Upsert` replaces one keyed row and emits ordered before/after changes; see [UPSERT_SOURCES.md](UPSERT_SOURCES.md).
- [x] M042 CDC envelope normalization. `TypedTableChange` carries normalized operation, key, before/after rows, and monotone sequence data for arrangements; see [CDC_ENVELOPES.md](CDC_ENVELOPES.md).
- [x] M043 Kafka-style source offset tracking.
- [x] M044 Source transaction grouping - `SQLSourceOffsetTracker.AdvanceTransaction` applies a distinct multi-partition source transaction only when every member is newer; see [SQL_SOURCE_TRANSACTION_GROUPING.md](SQL_SOURCE_TRANSACTION_GROUPING.md).
- [x] M045 Exactly-once source ingestion - `hatSql.SQLSourceIngestionCoordinator` provides an opt-in source transaction idempotency gate with complete offset validation, conflict detection, concurrent single-flight, retry cleanup, and snapshot/restore; durable atomic coupling with the applied transaction or an idempotent callback remains required; see [SQL_SOURCE_INGESTION.md](SQL_SOURCE_INGESTION.md).
- [x] M046 Sink progress and acknowledged frontiers - `SQLSinkProgressTracker` records monotone per-sink/partition acknowledgements with validated batch updates and deterministic snapshots; see [SQL_SINK_PROGRESS.md](SQL_SINK_PROGRESS.md).
- [x] M047 Exactly-once sink commits - `hatSql.SQLSinkCommitCoordinator` provides an opt-in idempotent sink commit gate with concurrent single-flight, conflict detection, retry cleanup, and snapshot/restore; atomic persistence with the sink or an idempotent sink callback remains required; see [SQL_SINK_COMMIT.md](SQL_SINK_COMMIT.md).
- [x] M048 Backpressure for asynchronous producers and subscribers.
- [x] M049 Operator cancellation through context propagation.
- [x] M050 Timely-style worker parallelism with deterministic merge. Added opt-in generic `hatPipeline.OrderedMap`, which claims input indexes concurrently and merges directly into stable result positions while canceling cooperative work on error; see [ORDERED_MAP.md](ORDERED_MAP.md). The cheap-work benchmark shows why it is not a default sequential-loop replacement.
- [x] M051 Compiled dataflow intermediate representation.
- [x] M051a Add an opt-in immutable compiled SQL query handle that reuses a parsed template across executions while preserving per-call cloning, binding, and the ordinary executor.
- [x] M051b Expose a fresh, deterministic logical dataflow IR snapshot from compiled SQL queries without changing execution, storage, or wire behavior; see [COMPILED_DATAFLOW_IR.md](COMPILED_DATAFLOW_IR.md).
- [ ] M052 Lowering SQL plans into reusable dataflow fragments with automatic
  built-in operator execution remains open.
- [x] M052a Lower compiled logical stages into a lazy, versioned immutable
  `SQLDataflowPlan` with defensive snapshots. M052b adds caller-supplied
  executable fragment composition. See
  [SQL_DATAFLOW_LOWERING.md](SQL_DATAFLOW_LOWERING.md).
- [x] M052b Provide caller-supplied executable reusable fragment composition
  with plan validation, defensive snapshots, cancellation checks, and no
  change to the existing SQL executor. See
  [SQL_DATAFLOW_EXECUTOR.md](SQL_DATAFLOW_EXECUTOR.md) and the benchmark in
  [BENCHMARK.md](BENCHMARK.md#reusable-sql-dataflow-fragment-execution).
- [x] M052c Add an opt-in built-in executor for single-source `CACHE`/`KEYS`
  scalar filter/project batches. `CompileNativeDataflow` preserves ordinary
  SQL semantics for its supported shape and returns
  `ErrSQLNativeDataflowUnsupported` for unsupported plans; see
  [SQL_DATAFLOW_EXECUTOR.md](SQL_DATAFLOW_EXECUTOR.md#built-in-native-batch-path)
  and [BENCHMARK.md](BENCHMARK.md#native-batch-measurement).
- [x] M053 Extensible optimizer rule framework. `SQLQueryOptimizer` provides an opt-in ordered rule hook over structural EXPLAIN steps and existing `SQLIndexHint` controls; nil keeps the default path. See [SQL_OPTIMIZER_RULES.md](SQL_OPTIMIZER_RULES.md) and the measured opt-in cost in [BENCHMARK.md](BENCHMARK.md#opt-in-sql-optimizer-rules).
- [x] M054 Predicate pushdown before arrangement maintenance.
- [x] M055 Projection pruning before arrangement maintenance.
- [x] M056 Join order selection for supported query shapes.
- [x] M057 Filter-before-arrange planning.
- [x] M058 Arrangement sharing.
- [x] M059 Index selection from predicate shape.
- [x] M059a Function predicates reach specialized multikey index resolvers before binary-only planner guards; `ARRAY_CONTAINS` correctness and before/after measurements are recorded in [BENCHMARK.md](BENCHMARK.md).
- [x] M060 View dependency graph and invalidation.
- [x] M061 Incremental view maintenance for supported typed views.
- [x] M062 Materialized view refresh and hydration state.
- [x] M063 Non-materialized SQL views.
- [ ] M064 Recursive dataflow maintenance.
- [x] M064a Append-only incremental transitive reachability with cycle-safe positive deltas; arbitrary deletes and updates remain rebuild-only. See [INCREMENTAL_RECURSIVE_REACHABILITY.md](INCREMENTAL_RECURSIVE_REACHABILITY.md).
- [x] M064b Opt-in Materialize-style recursive reachability `INSERT`/`UPDATE`/`DELETE` maintenance with exact signed pair retractions, affected-source recomputation, leaf direct fast paths, cycle tests, and before/after allocation benchmarks. See [INCREMENTAL_RECURSIVE_REACHABILITY.md](INCREMENTAL_RECURSIVE_REACHABILITY.md).
- [ ] M065 Incremental window-function maintenance.
- [x] M065a Append-only incremental `ROW_NUMBER`, `RANK`, and `DENSE_RANK` maintenance with atomic batch validation; arbitrary updates and retractions remain open. See [INCREMENTAL_RANK_WINDOW.md](INCREMENTAL_RANK_WINDOW.md).
- [x] M065b Opt-in Materialize-style rank-window `INSERT`/`UPDATE`/`DELETE` maintenance with signed retractions, affected-partition rebuilds, atomic validation, and before/after allocation benchmarks. Full framed window maintenance remains open. See [INCREMENTAL_RANK_WINDOW.md](INCREMENTAL_RANK_WINDOW.md).
- [x] M065c Append-only incremental `LAG`/`LEAD` maintenance with bounded per-partition history or unresolved tails, exact same-batch lookahead, atomic validation, and before/after CPU and allocation benchmarks. Arbitrary updates and deletes remain open. See [INCREMENTAL_OFFSET_WINDOW.md](INCREMENTAL_OFFSET_WINDOW.md).
- [x] M065d Append-only incremental bounded `ROWS BETWEEN N PRECEDING AND CURRENT ROW` maintenance for exact `COUNT(*)` and `SUM(int64)`, with SQL NULL behavior, checked overflow, atomic validation, and before/after CPU/allocation benchmarks. Arbitrary updates, deletes, peer-aware `RANGE` frames, and other aggregates remain open. See [INCREMENTAL_FRAME_WINDOW.md](INCREMENTAL_FRAME_WINDOW.md).
- [x] M065e Append-only incremental `FIRST_VALUE` and `LAST_VALUE` maintenance for `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, preserving SQL NULLs with O(1) per-partition state, atomic validation, and before/after CPU/allocation benchmarks. Arbitrary updates, deletes, `IGNORE NULLS`, peer-aware `RANGE` frames, and other window functions remain open. See [INCREMENTAL_BOUNDARY_WINDOW.md](INCREMENTAL_BOUNDARY_WINDOW.md).
- [x] M065f Append-only incremental fixed-position `NTH_VALUE` maintenance for `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, preserving SQL NULLs with O(1) per-partition state, atomic validation, and before/after CPU/allocation benchmarks. Arbitrary updates, deletes, dynamic positions, `IGNORE NULLS`, peer-aware `RANGE` frames, and other window functions remain open. See [INCREMENTAL_NTH_VALUE_WINDOW.md](INCREMENTAL_NTH_VALUE_WINDOW.md).
- [x] M065g Append-only incremental bounded `MIN(int64)` and `MAX(int64)` maintenance with NULL-aware monotonic deques, O(1) amortized updates, bounded per-partition state, atomic validation, and before/after CPU/allocation benchmarks. Arbitrary updates, deletes, non-`int64` values, peer-aware `RANGE` frames, and other window functions remain open. See [INCREMENTAL_EXTREMA_FRAME_WINDOW.md](INCREMENTAL_EXTREMA_FRAME_WINDOW.md).
- [x] M065h Append-only incremental bounded `AVG(int64)` maintenance with NULL-aware checked sum/count state, `float64` results, O(1) updates, atomic validation, and before/after CPU/allocation benchmarks. Arbitrary updates, deletes, unbounded-precision averages, peer-aware `RANGE` frames, and other window functions remain open. See [INCREMENTAL_AVERAGE_FRAME_WINDOW.md](INCREMENTAL_AVERAGE_FRAME_WINDOW.md).
- [x] M065i Append-only incremental bounded `COUNT(DISTINCT int64)` maintenance with NULL-aware reference-counted multiplicities, bounded per-partition state, atomic validation, and before/after CPU/allocation benchmarks. Arbitrary updates, deletes, non-`int64` values, peer-aware `RANGE` frames, and other window functions remain open. See [INCREMENTAL_DISTINCT_FRAME_WINDOW.md](INCREMENTAL_DISTINCT_FRAME_WINDOW.md).
- [ ] M065j ClickHouse-style small-cardinality arrangements for bounded `COUNT(DISTINCT)`. The first adaptive slice experiment was benchmarked and rolled back because it was only 4.4% faster while using 6.2% more transient bytes; see [BENCHMARK.md](BENCHMARK.md#rejected-m065j-small-cardinality-distinct-arrangement).
- [x] M065k Post-validation zero-copy state reuse for incremental `COUNT(DISTINCT int64)` avoids copying the bounded contribution queue and multiplicity map while preserving atomic validation; before/after CPU, bytes, and allocation measurements are recorded in [BENCHMARK.md](BENCHMARK.md#m065k-zero-copy-distinct-frame-state).
- [x] M065l Opt-in Materialize-style mutable bounded frame maintenance accepts exact row `INSERT`/`UPDATE`/`DELETE` operations for all six bounded frame aggregates, rebuilds only affected partitions, emits signed retractions/insertions, and preserves the append-only low-retention default. See [INCREMENTAL_MUTABLE_FRAME_WINDOW.md](INCREMENTAL_MUTABLE_FRAME_WINDOW.md) and [BENCHMARK.md](BENCHMARK.md#m065l-mutable-bounded-frame-windows).
- [x] M065m Opt-in peer-aware numeric `RANGE BETWEEN N PRECEDING AND CURRENT ROW` maintenance for `COUNT(*)` and `SUM(int64)` with exact peer replacements, descending order, atomic validation, and before/after CPU, bytes, and allocation measurements. See [INCREMENTAL_RANGE_WINDOW.md](INCREMENTAL_RANGE_WINDOW.md).
- [x] M066 Incremental sort maintenance via opt-in typed-table sorted arrangements, bulk batch rebuilds, deterministic NULL/NaN ordering, correctness tests, and benchmark guide.
- [x] M066a Strictly validated append-only bulk maintenance skips a full arrangement rebuild when incoming rows are already ordered after the current tail; mixed, duplicate, update, delete, gap, and out-of-order batches retain the existing rebuild or incremental path.
- [x] M066b Tail-checked single-row inserts append directly when the candidate is already after the current sorted tail; non-tail updates and inserts retain binary-search insertion.
- [x] M067 Top-K with offset for supported ordered paths.
- [x] M067a Bounded `RowsPage` snapshots for maintained typed sorted arrangements; the legacy `Rows()` snapshot remains unchanged, with correctness and allocation measurements in [BENCHMARK.md](BENCHMARK.md#typed-sorted-arrangement-page-snapshots).
- [x] M067b Add additive composite ordered arrangements with per-field direction, NULL placement, and optional string dictionary encoding; preserve the legacy single-field definition and validate duplicate or ambiguous order specifications, with construction measurements in [BENCHMARK.md](BENCHMARK.md#composite-sorted-arrangement-ordering).
- [x] M068 Differential group-by updates for generic SQL tables. `GroupCountDifferentialRows` and `GroupSumInt64DifferentialRows` maintain exact signed COUNT/SUM transitions for callback-defined groups, including negative/overflow validation and no partial output; see [DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md).
- [x] M068a Exact generic differential COUNT group maintenance.
- [x] M068b Exact signed int64 differential SUM group maintenance.
- [x] M068c Exact one-pass signed int64 differential COUNT+SUM group maintenance.
- [x] M069 Differential distinct updates for generic SQL tables. `DistinctDifferentialRows` maintains signed multiplicity transitions with boundary-only emissions, cloning, and negative/overflow validation; see [DIFFERENTIAL_DISTINCT.md](DIFFERENTIAL_DISTINCT.md).
- [x] M069a Boundary-only differential distinct maintenance.
- [x] M070 Monotone aggregate specialization. `TypedTableAggregate.ApplyMonotone` provides a tested insert-only fast path with sequence/replay validation and preserves advanced aggregate semantics; benchmarked at about `1.09x` faster with unchanged allocations; see [MONOTONE_AGGREGATE.md](MONOTONE_AGGREGATE.md).
- [x] M070a Append-only typed-table aggregate fast path.
- [x] M071 Late-data handling policy. `ClassifyLateData` provides allocation-free bounded-lateness decisions with explicit retain/drop behavior and inclusive boundaries; see [LATE_DATA_POLICY.md](LATE_DATA_POLICY.md).
- [x] M071a Bounded-lateness classifier with explicit drop-or-retain policy and boundary semantics.
- [x] M071a Explicit accept/reject/drop late-data policy.
- [x] M072 Watermark propagation. `MergeWatermarks`, `AdvanceWatermark`, and `DifferentialWatermark` provide safe minimum frontiers, monotone publication, and regression-checked differential application; see [WATERMARK.md](WATERMARK.md) and [DIFFERENTIAL_WATERMARK.md](DIFFERENTIAL_WATERMARK.md).
- [x] M072a Safe minimum-source watermark merge with monotonic publication.
- [x] M072a Monotone differential watermark propagation.
- [x] M073 Temporal joins. `DifferentialTemporalJoin` maintains weighted in-memory temporal inner equi-joins with per-key indexes, signed updates, cloning, and bounded time distance; see [DIFFERENTIAL_TEMPORAL_JOIN.md](DIFFERENTIAL_TEMPORAL_JOIN.md).
- [x] M073a Indexed weighted temporal equi-join.
- [x] M074 Interval joins. `DifferentialTemporalJoin` supports inclusive minimum and maximum timestamp distances while preserving max-only backward compatibility; see [DIFFERENTIAL_INTERVAL_JOIN.md](DIFFERENTIAL_INTERVAL_JOIN.md).
- [x] M074a Inclusive timestamp interval bounds for temporal joins.
- [x] M075 Temporal filtering for supported typed paths.

### Planning, Explainability, And Operations

- [x] M076 Explain plan output.
- [x] M077 Explain arrangement ownership and reuse. `TypedTableAggregateArrangements.Snapshot` and `TypedTableJoinArrangements.Snapshot` expose deterministic ownership, active lease reuse, checkpoints, source sequences, and staleness without changing execution.
- [x] M078 Explain the full dataflow graph. `BuildExplainDataflowGraph` preserves every EXPLAIN step, derives nested depth, and emits stable pipeline/subplan edges; JSON and DOT helpers are additive and read-only.
- [x] M079 Explain keys and index characteristics.
- [x] M080 Explain optimizer notices and rejected alternatives. `EXPLAIN ANALYZE` now exposes additive structured `ExplainAlternative` and `ExplainNotice` metadata on `INDEX CANDIDATES` steps while preserving existing plan text and result columns.
- [x] M081 Arrangement and source metrics for existing structures.
- [x] M082 Operator-level row and latency metrics where instrumented.
- [x] M083 Per-source lag and frontier metrics.
- [x] M083a Thread-safe monotone source frontier registry with deterministic lag snapshots.
- [x] M083a On-demand per-subscription frontier and lag status.
- [x] M084 Per-operator retained-memory metrics (see OPERATOR_MEMORY.md).
- [x] M084a Thread-safe operator retained-memory gauge registry with deterministic snapshots.
- [x] M085 Per-collection size and compaction metrics (see COLLECTION_METRICS.md).
- [x] M085a Thread-safe collection size gauges and compaction counters with deterministic snapshots.
- [x] M086 Replica isolation for independent workloads.
- [x] M087 Deterministic replica replay checks (see REPLAY_DIGEST.md).
- [x] M087a Canonical ordered replay digests with sequence validation and deterministic mismatch errors.
- [x] M088 Read replicas with explicit staleness bounds (see READ_REPLICA_POLICY.md).
- [x] M088a Deterministic read-replica selection with required frontiers and maximum lag.
- [x] M089 Failover and recovery of supported replicas.
- [ ] M090 Independent compute and storage scaling.
- [x] M091 Durable persistent shards through the local storage layer.
- [x] M092 Batched writes to durable storage.
- [x] M093 Caller-driven bounded persistent-shard compaction scheduling with duplicate request coalescing, deterministic task ordering, retry-preserving failures, and explicit concurrency limits; see [COMPACTION_SCHEDULER.md](COMPACTION_SCHEDULER.md).
- [x] M094 Persistent-shard leases and fencing; fully covered by the durable local lease implementation in M094a.
- [x] M094a Non-blocking local persistent-shard leases with separate advisory lock files, durable monotonic fencing tokens, atomic state publication, stale-token validation, renewal, inspection, and corruption/path-safety tests; see [PERSISTENT_SHARD_LEASES.md](PERSISTENT_SHARD_LEASES.md).
- [x] M095 Snapshot hydration with progress reporting.
- [x] M096 Per-column dictionary compression for arrangements.
- [x] M096a Deterministic low-cardinality string dictionary codec.
- [x] M096b Bounded dictionary admission avoids allocating row codes for columns that exceed the existing size bound; an eight-entry stack prefix preserves the low-cardinality path.
- [x] M096c Aggregate arrangements defer redundant serialized sort-key materialization until `Rows()` and invalidate only when a new group appears; correctness and cold/warm before/after measurements are recorded in [BENCHMARK.md](BENCHMARK.md#deferred-aggregate-arrangement-sort-keys).
- [x] M096d Add opt-in contiguous dictionary-value storage with immutable string data and offsets; keep legacy `Values` by default and validate packed offsets before SQL access.
- [x] M096e Add opt-in per-column dictionary-coded string group keys for typed aggregates, with reference-counted code reuse, partial-merge remapping, cached order references, and high-cardinality/repeated-group measurements in [BENCHMARK.md](BENCHMARK.md#per-column-dictionary-coded-aggregate-groups).
- [x] M096f Add opt-in reference-counted dictionary interning for live string sort values in typed sorted arrangements; preserve the legacy row shape and ordering while sharing repeated backing strings, with lifecycle tests and before/after measurements in [BENCHMARK.md](BENCHMARK.md#typed-sorted-arrangement-dictionary-interning).
- [x] M097 Compressed arrangement batches.
- [x] M097e Integrate the existing specialized packers into typed-table columnar caches behind CompressedBatches; keep the default off, charge packed payloads to MaxBytes, and record the memory/read tradeoff in [BENCHMARK.md](BENCHMARK.md#compressed-typed-table-columnar-batches).
- [x] M097a Add opt-in byte-packing for low-cardinality arrangement dictionary codes without changing logical values; the legacy representation remains the default for compatibility and predictable CPU cost.
- [x] M097b Add opt-in NULL validity-bitmap and dense-value packing for sparse nullable arrangement columns; pack only when the estimated retained layout is smaller, while preserving legacy `Columns` by default.
- [x] M097c Add opt-in bit-packing for boolean arrangement columns, using a second bitmap only when NULLs exist; preserve legacy `Columns` by default and reject non-boolean input.
- [x] M097d Add opt-in fixed-width `int64`/`float64` arrangement vectors with an optional NULL bitmap; preserve exact numeric types and special float bits, keep legacy `Columns` by default, and reject malformed metadata before SQL access.
- [x] M098 Schema evolution with compatibility checks.
- [x] M099 Zero-downtime migration and recovery documentation.
- [x] M100 Backup and restore verification.
- [x] M101 Audit and security controls.
- [x] M102 Role-based access control.
- [x] M103 Query timeout and cancellation.
- [x] M104 Retry-safe idempotent writes.
- [x] M105 Workload isolation and bounded queues.

## Tarantool Ideas

Tarantool contributes compact tuple/index layouts, operational replication
signals, transactional APIs, and simple queue primitives. The sharding items
remain unchecked or deferred because the current product direction favors
explicit regional partitioning and simple backups over automatic sharding.

### Tuples, Spaces, And Indexes

- [x] T001 Compact in-memory tuple representation for hot records.
- [x] T002 LSM-backed durable storage through the Pebble path.
- [x] T003 Space-like named collections.
- [x] T004 Typed tuple or row representation.
- [x] T005 Primary-key index.
- [x] T006 Ordered TREE index behavior.
- [x] T007 HASH index behavior.
- [x] T008 RTREE spatial index. Importable uint64-ID rectangle index with deterministic overlap and point search (see SPATIAL_RTREE.md).
- [x] T008a Adaptive sparse-grid spatial candidate enumeration - Keep grid-cell memory overhead while avoiding empty-cell scans and per-query candidate deduplication (see SPATIAL_INDEX.md).
- [x] T009 Bitset and bitmap structures.
- [x] T010 Functional indexes for supported expressions.
- [x] T011 Multikey indexes over array fields. `CreateSQLJSONMultikeyIndex` builds deduplicated element postings for `ARRAY_CONTAINS`; candidates are rechecked by the executor, mixed JSON types preserve existing SQL equality, and non-binary collation falls back to a scan. See [SQL_MULTIKEY_INDEX.md](SQL_MULTIKEY_INDEX.md).
- [x] T012 Partial equality indexes.
- [x] T013 Covering indexes and borrowed postings.
- [x] T014 Equality index iterators.
- [x] T015 Ordered range iterators.
- [x] T016 Partial-key search.
- [x] T017 Explicit NULL index semantics.
- [x] T018 Collation-aware string ordering.
- [x] T019 Unique constraints and duplicate-key errors.
- [x] T020 Online secondary-index build. `StartSQLJSONIndexRebuildWorker` provides an explicit background consumer for queued SQL JSON index rebuilds with immediate first polling, bounded one-unit ticks, cooperative stop/wait lifecycle, progress callbacks, and retry of failed work; no worker starts by default.
- [x] T021 Online index alteration with progress. `RunScheduledSQLJSONIndexRebuildsWithProgress` reports queue-level queued/running/completed/failed/canceled transitions and requeues canceled work for a later call while preserving atomic index publication; cancellation does not interrupt a single rebuild unit.
- [x] T022 Index statistics for selectivity estimates - `SQLJSONIndexStats` exposes cardinality/frequency statistics and `SQLJSONIndexValueEstimate` exposes exact posting estimates for configured JSON indexes.
- [x] T023 Persistent statistics refreshed from observed workloads. `SQLIndexAdvisor.Save` and `Load` persist bounded, versioned workload recommendations without SQL text, literal values, or row data; load validation is atomic and size-limited.
- [x] T024 Automatic covering-index recommendation. The opt-in `SQLIndexAdvisor` now exposes bounded `CoveringRecommendations` for slow, unindexed single-source equality projections; columns are canonicalized and returned without query text, literal values, or row data, while index creation remains an explicit operator decision.
- [x] T025 Index build cancellation and resume. `RunScheduledSQLJSONIndexRebuildsWithProgress` checks context between atomic rebuild units, emits a canceled transition, requeues the request, and resumes it on a later call without publishing a partial index; see [SQL_INDEX_REBUILD_PROGRESS.md](SQL_INDEX_REBUILD_PROGRESS.md).
- [x] T026 Index consistency checker independent of normal reads - `CheckSQLJSONIndexConsistency` rebuilds temporary candidates for every configured SQL JSON index kind, reports stale/unready state, and never repairs the live index.

### Transactions, WAL, Backup, And Recovery

- [x] T027 Consistent read views.
- [x] T028 Public multi-operation transaction API.
- [x] T029 Atomic callback or box.atomic-style mutation.
- [x] T030 Configurable transaction isolation levels.
- [x] T031 Savepoints and partial rollback.
- [x] T032 MVCC-style versioned reads for supported paths.
- [x] T033 Write-ahead journal.
- [x] T034 Durable snapshots.
- [x] T035 Checkpoints.
- [x] T036 Hot backup while serving reads and writes.
- [x] T037 Incremental WAL/journal backup.
- [x] T038 Backup checksums and manifest validation.
- [x] T039 Restore validation across all supported data types.
- [x] T040 Configurable WAL retention policy with disk budget.
- [x] T041 WAL segment compression and independent verification.
- [ ] T042 Recovery-time parallel replay. A bounded single-key parallel replay
  candidate was tested and rolled back because its 10,000-entry benchmark was
  1.25x slower and used 1.45x more heap than serial replay; see the rejected
  result in [BENCHMARK.md](BENCHMARK.md#rejected-recovery-time-parallel-replay).
- [x] T042a Recovery replay mutation fast path - scalar durable mutations avoid constructing public command responses; unsupported commands keep the existing dispatcher (see [JOURNAL_REPLAY.md](JOURNAL_REPLAY.md)).
- [x] T043 Recovery replay progress and ETA metrics.
- [x] T044 Recovery point selection by logical sequence.
- [x] T045 Crash-consistency fault injection.
- [x] T046 Online backup cancellation with resumable manifests.

### Replication And Topology

- [ ] T047 Synchronous replication with an explicit quorum. The public single-command path is now opt-in through `MonitoringOptions.WriteQuorum` / `CacheGRPCOptions.WriteQuorum`; atomic `BATCH` quorum semantics and rollback-free cluster-wide commit remain open.
- [x] T047d Atomic public `BATCH` write quorum. Eligible all-write batches validate direct quorum before mutation, preserve local atomic commit semantics when remote acknowledgements are insufficient, and send one grouped `INTERNALBATCHV2` per target; rollback-free cluster-wide commit remains open. See [WRITE_QUORUM.md](WRITE_QUORUM.md).
- [x] T047c Public HTTP and unary gRPC single-write commands enforce an opt-in direct write quorum at the shared command executor; `0` remains the default and asynchronous replication is rejected before mutation when quorum is enabled. See [WRITE_QUORUM.md](WRITE_QUORUM.md).
- [x] T047b HTTPReplicator exposes an opt-in direct ReplicateCommandWithQuorum API that counts the local result and remote acknowledgements without changing the asynchronous default.
- [x] T047a Explicit write-quorum decision helper with validation and acknowledgement reporting.
- [x] T048 Replication sets and peer topology.
- [x] T049 Vector-clock exposure for every replica - replication queue results expose an immutable observational `vector_clock` containing the local sequence and all current topology members' acknowledged sequences; it does not change quorum or conflict semantics.
- [x] T050 LSN or journal sequence exposure.
- [x] T051 Per-peer replication lag measurement: async queue status exposes source, acknowledged, and per-target lag sequences, with Prometheus gauges.
- [x] T052 Relay reconnect and retry.
- [x] T053 Bootstrap a node from a peer.
- [x] T054 Orphan replica detection and cleanup. Election status reports sorted liveness records whose node IDs are no longer in the current topology, and an authenticated `POST /api/election` with `{"cleanup_orphans":true}` prunes only those stale election records without touching cache or replica data.
- [x] T055 Idempotent journal replay.
- [x] T056 Deterministic conflict resolution for concurrent writers. `ConflictVersion` orders timestamp, node ID, and sequence deterministically; `ResolveConflictVersion` is covered by focused tests and remains separate from quorum/consensus transport. See [CONFLICT_RESOLUTION.md](CONFLICT_RESOLUTION.md).
- [x] T056a Deterministic conflict-version ordering with stable node and sequence tie-breaks.
- [x] T057 Election and failover behavior for supported topologies.
- [x] T058 Quorum reads and writes - `hatReplication.ExecuteReadQuorum` groups successful replica values against an explicit threshold, while `ExecuteWriteQuorum` gates named writes with the same explicit policy; both are opt-in caller-supplied transport primitives and leave normal asynchronous replication unchanged. See [READ_QUORUM.md](READ_QUORUM.md) and [WRITE_QUORUM.md](WRITE_QUORUM.md).
- [x] T058a Validated read/write quorum policy decisions with majority defaults (see QUORUM_POLICY.md).
- [x] T059 Read-only mode during failover.
- [x] T060 Replica health gate before serving stale-sensitive reads. Optional `RequireHealthyReplicaReads` / `-require-healthy-replica-reads` gates HTTP and native gRPC read commands and read-only typed batches on election health; the default remains off, and writes plus internal replication are unchanged.
- [x] T061 Replication queue depth and error metrics - `/metrics` exposes queue depth, capacity, enqueue/drop, attempt, success/failure, retry, and age gauges/counters.
- [x] T062 Replication bandwidth and compression metrics - `/metrics` exposes outgoing request wire bytes and request counts by target and `identity`/`gzip` encoding; see [REPLICATION_METRICS.md](REPLICATION_METRICS.md).
- [x] T063 Replication pause and resume controls - async replication exposes idempotent Go pause/resume methods, authenticated `/api/replication` actions, queue status, and a Prometheus paused gauge; see [REPLICATION_OPERATIONS.md](REPLICATION_OPERATIONS.md).
- [x] T064 Rolling replica replacement. `cluster add-replica -replace` catches the replacement node up before activation, while `cluster decommission` validates remaining redundancy, runs a final sync, marks the retiring node offline, updates reachable members, and verifies the resulting topology.
- [x] T065 Failure-domain-aware replica placement. Topology nodes carry optional `failure_domain` metadata through JSON and native gRPC; `cluster add-replica` and `cluster join` accept `-failure-domain` plus opt-in `-min-failure-domains N` (default `0`) and reject placements that do not meet the requested distinct-domain count.
- [x] T066 Cross-region replication with explicit RPO/RTO. Topology nodes carry optional region metadata through JSON and native gRPC; an opt-in replication policy reports required-region coverage and journal-sequence RPO lag, and validates declared RTO durations. The default is disabled, with no automatic cross-region routing or failover.
- [x] T067 Split-brain fencing token. An optional non-zero topology `fencing_token` is monotonic in `TopologyStore`, included in the topology fingerprint and native gRPC topology, attached to replication writes and batch envelopes, and mismatched or missing non-zero tokens are rejected before apply; token `0` preserves legacy behavior. This is operator-controlled stale-writer fencing, not quorum consensus.
- [x] T068 Schema-change replication compatibility checks. `hatSchema.Schema.Fingerprint` is deterministic across source-map order, and an opt-in version plus fingerprint contract is carried through HTTP command/batch/compact-sync and native gRPC replication; receivers reject missing, malformed, or mismatched contracts before apply while the default remains off.
- [x] T069 Recovery rehearsal that compares checksums after replay.

### Partitioning And Sharding

- [-] T070 Automatic vshard-style routers - deferred in favor of explicit regional partitioning.
- [-] T071 Storage replica sets behind stateless routers - deferred with automatic sharding.
- [-] T072 Virtual buckets - deferred until backup and movement semantics are specified.
- [-] T073 Automatic bucket rebalancing - deferred until operator controls are complete.
- [-] T074 Router failover and discovery - deferred with automatic sharding.
- [-] T075 Online partition migration - proposal required before implementation.
- [x] T076 Explicit region partition routing. Implemented by the immutable
  longest-prefix `hatPartition.PrefixRouter`; unmatched keys remain explicitly
  unassigned and the routing rules are validated before publication. See
  [REGIONAL_PARTITION_ROUTING.md](REGIONAL_PARTITION_ROUTING.md).
- [x] T076a Immutable longest-prefix routing for explicit region keys. `hatPartition.PrefixRouter` validates normalized rules, chooses the most specific prefix without lookup allocation, and leaves unmatched keys unassigned; see [REGIONAL_PARTITION_ROUTING.md](REGIONAL_PARTITION_ROUTING.md).
- [x] T077 Region-local backup and restore. `partition_local` snapshot bundles filter records before encoding, carry `partition.local` metadata, are validated by the backup doctor, and require an optional matching partition selector before restore publication; Pebble checkpoint and incremental modes remain whole-store artifacts.
- [x] T078 Cross-region read policy. `hatReplication.SelectReadReplicaWithConsistency`
  applies freshness constraints before optional ordered region preferences and
  falls back deterministically when the preferred region is unavailable. See
  [REPLICA_LOCALITY_ROUTING.md](REPLICA_LOCALITY_ROUTING.md) and
  [READ_CONSISTENCY.md](READ_CONSISTENCY.md).
- [x] T079 Partition ownership and fencing metadata. `hatTopology.PartitionOwnership` exposes a stable primary/replica snapshot bound to the existing topology fingerprint and fencing token; `TopologyStore` forwards snapshot and write-validation APIs through zero-retained-memory normalized fast paths without changing legacy topology JSON or command behavior. See [PARTITION_OWNERSHIP.md](PARTITION_OWNERSHIP.md).
- [x] T080 Deterministic partition split and merge planning via the importable
  `hatPartition.PlanSplit`, `PlanMerge`, and `PlanResize` API. It computes
  allocation-free per-key source/target routes but does not perform online
  migration; that remains deferred under T075. See [PARTITION_RESIZE.md](PARTITION_RESIZE.md).
- [x] T081 Partition-local query planning. `hatSql.PartitionPruningSourceResolver`
  participates in planning before source scans and preserves the full
  predicate for post-pruning re-evaluation; unsupported predicates retain the
  legacy path. See [PARTITION_PRUNING.md](PARTITION_PRUNING.md).
- [x] T082 Partition pruning from region predicates. Literal `region =` and
  `region IN (...)` predicates select complete physical partition subsets while
  preserving correctness through the original `WHERE` evaluation; see
  [PARTITION_PRUNING.md](PARTITION_PRUNING.md).
- [x] T083 Cross-partition aggregate merge. `hatSql.TypedTableAggregate.MergePartial`
  and `MergePartials` combine validated partition-local COUNT, SUM, MIN, MAX,
  and COUNT DISTINCT states deterministically; see
  [DISTRIBUTED_PARTIAL_AGGREGATION.md](DISTRIBUTED_PARTIAL_AGGREGATION.md).
- [x] T084 Cross-partition ordered pagination. The opt-in
  `PartitionedOrderedSourceResolver` performs a deterministic k-way merge for
  keyset pages over independently ordered physical partitions, with a cursor
  that records each partition's progress and rejects layout changes. The
  legacy offset and direct-source keyset paths remain unchanged. See
  [CROSS_PARTITION_PAGINATION.md](CROSS_PARTITION_PAGINATION.md) and the raw
  measurements in [BENCHMARK.md](BENCHMARK.md#cross-partition-ordered-keyset-pagination).
- [x] T085 Partition health and lag dashboard. The Svelte MPA Admin page
  derives read-only per-partition primary, region, replica count, maximum
  reported sequence lag, maintenance, and unknown states from `/api/topology`
  and `/api/replication`; no routing, failover, or default configuration
  changes are made. See [PARTITION_HEALTH.md](PARTITION_HEALTH.md).

### Queues, Calls, And Runtime

- [x] T086 Queue spaces for FIFO workloads.
- [x] T087 FIFO queue operations.
- [x] T088 Priority queue operations.
- [x] T089 Delay queue operations - public generic `hatDataStructure.DelayQueue` uses a stable 4-ary deadline heap with zero steady-state allocations and `PopReady`/`NextReadyAt` operations. See [DELAY_QUEUE.md](DELAY_QUEUE.md).
- [x] T090 TTL queue expiration.
- [x] T091 Scheduled refresh and maintenance tasks.
- [x] T092 Fiber-style cooperative scheduler. `hat/hatPipeline.Scheduler` provides fixed workers, bounded cooperative task submission, cancellation, fail-fast errors, and close/drain lifecycle semantics. See [SCHEDULER.md](SCHEDULER.md).
- [x] T093 Cooperative yielding in bounded worker loops.
- [x] T094 Channels for typed producer-consumer exchange. `hat/hatPipeline.Channel[T]` provides bounded buffering, context-aware send/receive, and idempotent close with drain semantics. See [CHANNELS.md](CHANNELS.md).
- [x] T095 Net.box-like binary client path.
- [x] T096 IProto-like compact protocol path.
- [x] T097 Prepared calls and prepared SQL statements.
- [x] T098 Batched requests.
- [x] T099 Request pipelining.
- [x] T100 Streaming query results.
- [x] T101 Response backpressure.
- [x] T102 Stored functions/UDFs for supported languages.
- [ ] T103 Native FFI extension boundary.
- [x] T104 Sandboxed UDF execution.
- [x] T105 Hot module loading with version checks. Added an opt-in in-process `hatSql.PluginRegistry` with atomic version-checked load/replace/unload, monotonic generations, and deterministic metadata snapshots; see [PLUGIN_REGISTRY.md](PLUGIN_REGISTRY.md). Native shared-library loading remains intentionally out of scope for the security boundary.
- [x] T106 Triggers and update hooks for supported collections.
- [x] T107 Replace hooks for journal and projection maintenance.
- [x] T108 Transactional trigger ordering guarantees. Added `hatSql.SQLTriggerRegistry` and `SQLTriggerTransaction` with deterministic event/trigger ordering, prepare-before-apply, primary-first commit, reverse rollback, and explicit caller-owned atomicity; see [SQL_TRIGGERS.md](SQL_TRIGGERS.md). SQL parser `CREATE TRIGGER` wiring remains a separate T120 item.
- [x] T109 Runtime configuration with validation.
- [x] T110 Memory quotas and admission limits.
- [x] T111 Separate cache sizing from durable-storage sizing. Added independent `-cache-memory-cap-bytes` and `-db-storage-max-bytes` controls, with the old cache flag retained as a legacy alias; see the README and `BENCHMARK.md`.
- [x] T112 Per-queue memory and latency metrics - async replication exposes estimated resident queued/in-flight payload bytes plus queue wait/service histograms through status and Prometheus; see [REPLICATION_OPERATIONS.md](REPLICATION_OPERATIONS.md).
- [x] T113 Dead-letter queue with replay controls - public generic bounded retention supports inspection, explicit replay deadlines, and discard; see [DEAD_LETTER_QUEUE.md](DEAD_LETTER_QUEUE.md).
- [x] T114 Work stealing for independent queue workers. Added the opt-in `hatPipeline.WorkStealingPool` with bounded total queue capacity, queue-local owner pops, victim steals, cancellation, drain-on-close, and first-error propagation; see [WORK_STEALING.md](WORK_STEALING.md). It is not a replacement for the lower-overhead shared-queue scheduler for tiny balanced tasks.
- [x] T115 Cancellation-safe task ownership: `HTTPReplicator.CloseWithContext` drains asynchronous work without dropping owned tasks when a shutdown deadline expires.

### SQL, Security, And Operations

- [x] T116 SQL over named collections.
- [x] T117 Explain query plan output.
- [x] T118 Parameter binding.
- [x] T119 SQL views.
- [x] T120 SQL triggers with transaction semantics. Strict `CREATE TRIGGER`
  parsing and opt-in direct SQL DML dispatch use prepare-before-apply,
  deterministic ordering, and rollback on trigger commit failure; see
  [SQL_TRIGGERS.md](SQL_TRIGGERS.md).
- [x] T120a Strict row-level `CREATE TRIGGER` parsing and explicit registry registration for `AFTER` DML events; automatic DML wiring remains caller-owned, see [SQL_TRIGGERS.md](SQL_TRIGGERS.md).
- [x] T120b Opt-in automatic row-level trigger dispatch for direct SQL INSERT, UPDATE, and DELETE with prepare-before-apply, rollback on trigger commit failure, and legacy default-off behavior; see [SQL_TRIGGERS.md](SQL_TRIGGERS.md).
- [x] T121 Public SQL transaction commands. `CompileSQL` exposes `BEGIN ATOMIC` programs with savepoints, and `BeginSQLTransaction` exposes snapshot reads, rollback, conflict-aware commit, and savepoint methods.
- [x] T122 UPSERT behavior.
- [x] T123 REPLACE behavior.
- [x] T124 DELETE behavior.
- [x] T125 UPDATE behavior.
- [x] T126 RETURNING clauses. `ExecuteSQLMutation` returns selected key/value/existence/TTL columns for direct key-targeted mutations, including delete and conditional merge results.
- [x] T127 ON CONFLICT clauses. `ExecuteSQLMutation` supports primary-key `DO NOTHING` and typed `DO UPDATE ... EXCLUDED` forms with explicit rejection of unsupported expressions and expiration combinations; the supported paths are covered by tests and a five-run benchmark.
- [x] T128 MERGE statements. `ExecuteSQLMutation` supports matched and not-matched conditional `MERGE` actions through the atomic merge executor, with `RETURNING` coverage.
- [x] T129 Common table expressions.
- [x] T130 Generated columns. `TypedTableColumn.Generated` computes a typed value before storage and changefeed publication; derived values are visible through row/columnar SQL, MVCC, and replay paths. See [GENERATED_COLUMNS.md](GENERATED_COLUMNS.md).
- [x] T131 Typed constraints.
- [x] T132 Foreign-key enforcement - `hatSchema.ValidateDataset` and `ValidateRows` validate declared foreign keys against an immutable dataset view or caller-supplied source resolver, including composite keys and NULL semantics; enforcement remains explicit at the caller's transaction boundary. See [SCHEMA_CONSTRAINTS.md](SCHEMA_CONSTRAINTS.md).
- [x] T133 JSON path access.
- [x] T134 Spatial predicates. SQL now supports `GEO_DISTANCE_METERS`/`GEO_DISTANCE`, `GEO_WITHIN_RADIUS`, and dateline-aware `GEO_WITHIN_BOX` with NULL propagation and coordinate validation. See [SPATIAL_SQL.md](SPATIAL_SQL.md).
- [x] T135 Replication and memory introspection.
- [x] T136 Fiber and scheduler introspection - Add an authenticated `/api/scheduler` report and Prometheus gauges for goroutine, GOMAXPROCS, CPU, and scheduler metric state; the on-demand report is zero-allocation in the package benchmark (see SCHEDULER_MONITORING.md).
- [x] T137 Health checks.
- [x] T138 Topology introspection.
- [x] T139 Hot-reloadable safe configuration.
- [x] T140 Rolling restart procedures.
- [x] T141 TLS and authentication.
- [x] T142 Authorization and audit logging.
- [x] T143 Rate limiting.
- [x] T144 Resource isolation.
- [x] T145 Admin console with read-only diagnostics. The Svelte MPA exposes `/admin.html` with health, storage, audit, and replication diagnostics; its mutating flush/compact controls remain separately authenticated and protected.
- [x] T146 Operator command idempotency keys. Journal-backed synchronous, batch, and asynchronous command paths validate and fingerprint `CacheCommandRequest.IdempotencyKey`, replay the durable response for duplicates, and reject payload changes under the same key.
- [x] T147 Structured error codes for automation.
- [x] T148 CLI output formats for scripts and humans - the CLI keeps compact JSON as the default and supports opt-in streaming pretty JSON; see [CLI_OUTPUT.md](CLI_OUTPUT.md).
- [x] T149 Upgrade compatibility tests.
- [ ] T150 Language-neutral client SDK coverage.
- [x] T150a Public importable gRPC client aliases over the language-neutral protobuf contract.
- [x] T151 Online backup drill.
- [x] T152 Chaos tests for replication and recovery.
- [x] T153 Load-shedding policy under memory pressure. Covered by configurable persistent-store admission limits, RSS/hot-value cold eviction, and pressure-triggered TTL vacuum; command rejection remains intentionally caller-controlled because making it automatic would trade availability for protection.
- [x] T154 Automatic slow-command capture.
- [x] T155 Per-command allocation budgets.

## Adoption Workflow

For each future unchecked item:

1. Verify that the capability is not already present under another local name.
2. Record the workload, correctness contract, expected win, and operational
   cost before coding.
3. Add a focused failing test and run it through the repository Makefile.
4. Implement the smallest compatible version with the default behavior unchanged
   unless the measured result justifies a default change.
5. Run focused tests, race tests where relevant, the full test suite, and a
   repeatable benchmark with CPU, allocations, retained memory, and bandwidth.
6. Keep the change only when correctness holds and the measured tradeoff is
   acceptable; otherwise revert the feature and retain the regression test or
   benchmark note as appropriate.
7. Update this checklist and `ADOPTED_QUERY_ENGINE_IDEAS.md`, then commit and
   push the completed feature as its own change.

- [x] C016a Bounded independently scheduled pipeline stages with backpressure and cancellation.

- [x] C030a Width-aware adaptive dictionary selection for compact columnar layouts.

- [x] C029a Vertical columnar merge loads only requested fields from each part.

- [x] C038a TTL pruning removes only complete rollup buckets at explicit boundaries.
- [x] M051c immutable compiled SQL template reuse for static, parameter-free compiled handles; dynamic options retain the clone path. See [COMPILED_TEMPLATE_REUSE.md](COMPILED_TEMPLATE_REUSE.md) and [BENCHMARK.md](BENCHMARK.md#immutable-compiled-sql-template-reuse).
- [x] M065n Peer-aware numeric `RANGE` `MIN`/`MAX` maintenance using monotonic
  deques, including NULL handling, descending order, and atomic validation.
- [x] M065o Peer-aware numeric `RANGE` `COUNT(DISTINCT int64)` maintenance
  using exact multiplicity counts, NULL handling, descending order, and
  deterministic reference coverage.
- [x] M065p Peer-aware numeric `RANGE` `AVG(int64)` maintenance reusing
  checked sum/count state, with NULL, peer, descending, overflow, and
  deterministic reference coverage.
- [x] M065q Generic peer-aware numeric `RANGE` `FIRST_VALUE`/`LAST_VALUE`
  maintenance, with NULL-respecting values, peer retractions, expiry,
  descending order, and partitioned reference coverage.
- [x] M065r Generic peer-aware numeric `RANGE` `NTH_VALUE` maintenance with a
  fixed position, NULL-respecting values, peer retractions, int64 bound
  saturation, atomic validation, and partitioned reference coverage. The
  optimized path measured 2.89x faster than materialized evaluation with 65.0%
  higher cumulative bytes; see [INCREMENTAL_RANGE_WINDOW.md](INCREMENTAL_RANGE_WINDOW.md)
  and [BENCHMARK.md](BENCHMARK.md).
