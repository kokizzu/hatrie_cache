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
- [ ] C015 SIMD kernels for common numeric and string predicates.
- [ ] C016 Pipeline stages with independently scheduled work.
- [x] C017 EXPLAIN PIPELINE output with stage and worker detail. `EXPLAIN PIPELINE` now emits additive one-based stage, worker, and worker-count metadata derived from the static plan; regular `EXPLAIN` output remains unchanged and `EXPLAIN PIPELINE ANALYZE` is rejected explicitly.
- [x] C018 EXPLAIN output for the local SQL plan.
- [x] C019 Version-aware bounded query-result cache - the generic epoch-validated `ResultCache` already exists; automatic SQL wiring remains a separate question.
- [x] C020 Query-condition cache for reusable predicate state.
- [x] C021 Explicit projections for reusable sorted or aggregated data.
- [x] C022 Automatic selection of compatible projections.
- [x] C023 Materialized projection maintenance.
- [x] C024 Refreshable materialized views with an explicit refresh policy. `MaterializedViews.RefreshChanged`, `ManagedRefreshScheduler.AddMaterializedView`, and the disabled-by-default `IncrementalProjectionRunner` provide dependency-scoped refresh, fixed intervals, source-version checks, journal coalescing, and durable checkpoints. Verified with `make test-sql-refresh-scheduler`, `make test-sql-incremental-projection`, and `make benchmark-sql-incremental-projection`.
- [x] C025 Projection consistency checks against source versions.
- [x] C026 Independent persistent data parts through the Pebble-backed path.
- [x] C027 Part metadata and checksums.
- [x] C028 Background merge and compaction.
- [ ] C029 Vertical merge that reads only changed columns.
- [ ] C030 Compact-part format selected by row count and width.
- [ ] C031 ReplacingMergeTree-style latest-row replacement.
- [x] C031a Explicit stable-order replacing merge for versioned rows.
- [ ] C032 CollapsingMergeTree-style sign-based row cancellation.
- [x] C032a Explicit deterministic signed-row cancellation merge.
- [ ] C033 SummingMergeTree-style merge-time summation.
- [x] C033a Explicit overflow-checked summing merge for selected numeric columns.
- [x] C034 Aggregating states for reusable grouped results.
- [x] C035 Lightweight delete patch parts.
- [x] C036 Durable mutation queue with observable progress. `CommandJournal` and the LevelDB-backed `ReplicationOutboxStore` provide durable FIFO mutation recovery, dead-letter handling, bounded restore, and binary/JSON compatibility; `ReplayWithProgress` exposes concurrent replay progress and ETA. Verified with `make test-c036-durable-mutation-queue`, `make test-replay-progress`, `make test-race-replay-progress`, and `make benchmark-replay-progress`.
- [x] C037 TTL expiration for supported values and records.
- [ ] C038 TTL-driven rollup or recompression.
- [x] C039 Partition pruning for local partitions.
- [ ] C040 Sampling key with deterministic SAMPLE semantics across partitions.
- [x] C040a Deterministic key-hash sampling across partition boundaries.
- [ ] C041 Multiple disk policies with placement rules.
- [ ] C042 Hot, warm, and cold storage tiers.
- [ ] C043 Remote object-store parts with local metadata.
- [ ] C044 Zero-copy replication of immutable parts.
- [ ] C045 Part-level cache admission and eviction policy.
- [ ] C046 Read amplification accounting per part and column.
- [x] C046a Statistics-only per-column RowBinary read accounting.
- [ ] C047 Adaptive granule sizing from observed predicate selectivity.
- [x] C047a Conservative block min/max predicate pruning.
- [ ] C048 Compact numeric encodings selected from data statistics.
- [x] C048a Exact full-batch codec selection by encoded size.
- [x] C049 Low-cardinality dictionary encoding for typed string values.
- [ ] C050 Shared JSON subcolumns for repeated paths.
- [x] C051 JSON path indexing for supported predicates.
- [ ] C052 Native array and nested-column physical layout.
- [ ] C053 Bitmap-backed nullable columns.
- [x] C053a Nullable-column bitmap RowBinary format.
- [x] C054 Fixed-width date and datetime encodings - RowBinary stores dates as 4-byte epoch days and datetimes as 8-byte Unix nanoseconds, with explicit round-trip and payload-size verification (see ROW_BINARY.md).
- [x] C055 Delta encoding for monotonically changing numeric columns.
- [x] C056 Double-delta encoding for timestamps.
- [ ] C057 Gorilla-style floating-point encoding.
- [x] C058 Configurable storage compression codecs.
- [ ] C059 Codec selection from sampled column entropy.
- [x] C059a Sampled adaptive codec selection from a bounded prefix.
- [ ] C060 Compression ratio and decompression CPU accounting.
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
- [ ] C077 Aggregate combinators for reusable state, merge, and finalize phases.
- [x] C078 Top-K aggregation.
- [x] C079 Dynamic Top-N skipping.
- [x] C080 LIMIT BY per-group limiting.
- [ ] C081 WITH FILL and gap filling for ordered time series.
- [x] C082 Window functions for the supported SQL subset.
- [ ] C083 ArrayJoin-style row expansion.
- [ ] C084 Array and map aggregate functions.
- [x] C085 Hash joins.
- [x] C086 Grace-hash or spillable joins.
- [x] C087 Runtime Bloom filters for joins.
- [ ] C088 Partial-merge joins for sorted sources.
- [x] C089 Direct lookup joins through indexed sources.
- [x] C090 Join reordering for compatible query shapes.
- [ ] C091 Distributed joins with explicit data movement accounting.
- [x] C092 Semi-join and anti-join execution where supported.
- [ ] C093 Distributed partial aggregation followed by merge.
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
- [ ] C106 Workload classes with separate concurrency and memory budgets.
- [ ] C107 Admission control before expensive scans.
- [x] C107a Bounded query-admission queues - Cap namespace waiters before allocation while preserving the default unlimited behavior (see QUERY_GOVERNANCE.md).
- [x] C108 Kill-query command with an operator-visible reason. `SQLQueryManager.Cancel` provides bounded, reasoned cooperative cancellation; remote layers must apply their existing auth policy.
- [x] C109 Per-operator CPU and row counters. `SQLQueryEvent.Operators` reports privacy-safe input/output rows, bytes, and elapsed nanoseconds for each observed operator; existing observer tests cover the counters.
- [x] C110 Query pipeline trace export. `NewQueryTraceRecorder` provides an opt-in bounded observer with independent snapshots and JSONL export, retaining only the newest events when a positive limit is configured; focused tests cover bounds, cloning, and writer errors.

### Ingestion, Formats, And Operations

- [x] C111 Asynchronous inserts with bounded queues.
- [x] C112 Wait-for-async-insert acknowledgement mode.
- [x] C113 Idempotent insert deduplication.
- [x] C114 Adaptive async-insert batching.
- [ ] C115 Parallel input parsing with deterministic error reporting.
- [x] C116 Binary wire format negotiation.
- [x] C117 RowBinary-compatible encoding. `EncodeSQLRowBinary` and `DecodeSQLRowBinary` provide an explicit schema-aware RowBinary-style stream with little-endian fixed-width values, varint-length strings/bytes, nullable markers, strict validation, and bounded decoding; JSON/protobuf defaults remain unchanged. See [SQL_ROW_BINARY.md](SQL_ROW_BINARY.md).
- [x] C118 JSON row encoding for compatibility paths.
- [x] C119 Arrow-compatible column transfer.
- [x] C120 Parquet import and export. `ExternalTables` provides `ExportParquet`, `ImportParquet`, and `WriteParquet` over the existing in-memory external-table registry; round-trip tests and `BenchmarkExternalTablesExportTransfer` cover the path.
- [x] C121 Native compressed block transfer with independent blocks. `hatCodec.EncodeCompressedBlocks` and `DecodeCompressedBlocks` provide an opt-in `HCB1` stream with independently bounded raw/DEFLATE blocks, raw fallback, and CRC32 validation; JSON/protobuf/gzip defaults remain unchanged. The measured tradeoff is faster encoding but larger wire output and slower decoding than one gzip stream. See [COMPRESSED_BLOCKS.md](COMPRESSED_BLOCKS.md).
- [x] C122 Configurable wire compression.
- [ ] C123 Compression level negotiation per client.
- [x] C124 Column statistics in wire metadata. `BuildSQLRowBinaryColumnStats`, `EncodeSQLRowBinaryWithStats`, and `DecodeSQLRowBinaryWithStats` provide exact NULL/value counts and typed min/max metadata in an opt-in `HBS1` envelope; decoding recomputes and validates the metadata before returning it, while plain RowBinary remains unchanged. See [ROW_BINARY_STATS.md](ROW_BINARY_STATS.md).
- [x] C125 Dictionary transfer reuse across batches. `NewSQLRowBinaryDictionaryEncoder` and `NewSQLRowBinaryDictionaryDecoder` retain selected string/bytes/JSON dictionaries across schema-compatible batches, send only additions plus compact ids, bound retained state, and leave plain RowBinary unchanged. Benchmarks cover first/reused encode/decode paths and show the bandwidth/CPU tradeoff. See [ROW_BINARY_DICTIONARY.md](ROW_BINARY_DICTIONARY.md).
- [x] C126 Backup snapshots with checksums.
- [x] C127 Incremental journal-backed backup.
- [ ] C128 Object-store backup targets.
- [x] C129 Restore verification for supported data types.
- [x] C130 Recovery checkpoints and replay validation.
- [x] C131 Metrics and health endpoints are opt-in.
- [ ] C132 System tables for parts, mutations, and query history.
- [x] C133 Query log retention and sampling policy - SQLQueryManager provides bounded privacy-safe history with deterministic configurable completion sampling and no SQL-text retention (see QUERY_HISTORY.md).
- [x] C134 OpenTelemetry spans for query phases. `QueryTraceRecorder.OpenTelemetrySpans` exposes SDK-neutral root/query and operator child spans with OTLP-width IDs, status, counters, cloned attributes, and documented end-anchored phase timing. It is opt-in and keeps SQL/error/row data out of exported spans; see [QUERY_TRACING.md](QUERY_TRACING.md).
- [ ] C135 Trace IDs carried through remote work.
- [x] C136 Per-tenant resource quotas - NamespaceQueryGovernor applies immutable, tightening per-namespace resource policies suitable for tenant, user, or source isolation (see TENANT_RESOURCE_LIMITS.md).
- [x] C137 TLS, authentication, and authorization controls.
- [x] C138 Operational config validation with sane defaults.
- [x] C139 Regression and compatibility test matrix.
- [x] C140 Benchmark reports with raw results and memory measurements.

### Distributed And Replicated Behavior

- [ ] C141 Distributed table abstraction over partitions.
- [ ] C142 Automatic shard pruning from partition predicates.
- [ ] C143 Parallel replicas for one query.
- [ ] C144 Hedged reads for slow replicas.
- [ ] C145 Remote read retries with bounded duplicate work.
- [ ] C146 Distributed partial aggregation and final merge.
- [ ] C147 Quorum inserts with explicit durability policy.
- [ ] C148 Replicated part exchange with checksums.
- [x] C148a Immutable-part length and SHA-256 checksums.
- [x] C149 Replication queue introspection. `ReplicationResult.Queue` exposes `QueueStats` with depth, capacity, attempts, acknowledgements, failures, dead letters, pause state, and vector-clock state; monitoring also exports the queue health metrics.
- [ ] C150 Replica lag thresholds for read routing.
- [ ] C151 Read-after-write consistency levels.
- [ ] C152 Leader election independent from query workers.
- [ ] C153 Metadata consensus for partition ownership.
- [ ] C154 Rolling schema changes across replicas.
- [ ] C155 Rolling binary upgrades with compatibility gates.
- [ ] C156 Cross-region replication policy.
- [ ] C157 Cross-region backup restore drill.
- [ ] C158 Split-brain fencing.
- [ ] C159 Failure-domain-aware replica placement.
- [ ] C160 Query routing by region and locality.

## Materialize Ideas

Materialize contributes ideas about incremental dataflow, arrangements, logical
time, and self-correcting results. Items marked adopted refer to local behavior
with equivalent safety properties; they do not claim implementation parity with
Materialize's Timely/Differential Dataflow runtime.

### Dataflow, Arrangements, And Incremental Results

- [x] M001 Incremental data-parallel dataflow for supported table paths.
- [ ] M002 Generic `(data,time,diff)` multiset representation.
- [x] M002a Exported differential row batch representation.
- [ ] M003 Timely-style nested worker scopes.
- [x] M004 Data-parallel map, filter, project, and reduce stages where supported.
- [x] M005 Shared arrangements reused by multiple compatible queries.
- [x] M006 Arrangement keys derived from indexed predicates.
- [x] M007 Arrangement reuse across subscriptions and point reads.
- [x] M008 Arrangement compaction for old versions.
- [ ] M009 Consolidation of equal data and opposite diffs.
- [x] M009a Overflow-safe batch consolidation by key and time.
- [x] M010 Reduce/group arrangements for typed tables.
- [x] M011 Join arrangements for typed indexed tables.
- [x] M012 Distinct arrangements for typed sources.
- [x] M013 Top-K arrangements for ordered subscriptions.
- [ ] M014 Lookup arrangements for external or remote sources.
- [ ] M015 Delta joins that avoid repeated large-side scans.
- [ ] M016 Linear joins with explicit maintained indexes.
- [ ] M017 Semijoin reduction before maintaining a join.
- [ ] M018 Monotonicity analysis for cheaper maintenance.
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
- [ ] M033 Timestamp oracle for globally ordered writes.
- [ ] M034 Epoch management for restarts and leases.
- [x] M035 Self-correcting materialized results for typed arrangements.
- [x] M036 Retractions and insertions on typed updates.
- [ ] M037 Generic negative-diff support for every SQL operator.
- [x] M037a Signed negative diffs in the reusable batch primitive.
- [ ] M038 Generic multiset duplicate preservation across all operators.
- [x] M038a Duplicate multiplicity retained as signed diff weights.
- [x] M039 Compaction constrained by active read frontiers.
- [ ] M040 Append-only fast path selected from source metadata.
- [ ] M041 Upsert-source semantics with key replacement.
- [ ] M042 CDC envelope normalization.
- [ ] M043 Kafka-style source offset tracking.
- [ ] M044 Source transaction grouping.
- [ ] M045 Exactly-once source ingestion.
- [ ] M046 Sink progress and acknowledged frontiers.
- [ ] M047 Exactly-once sink commits.
- [x] M048 Backpressure for asynchronous producers and subscribers.
- [x] M049 Operator cancellation through context propagation.
- [ ] M050 Timely-style worker parallelism with deterministic merge.
- [ ] M051 Compiled dataflow intermediate representation.
- [ ] M052 Lowering SQL plans into reusable dataflow fragments.
- [ ] M053 Extensible optimizer rule framework.
- [x] M054 Predicate pushdown before arrangement maintenance.
- [x] M055 Projection pruning before arrangement maintenance.
- [x] M056 Join order selection for supported query shapes.
- [x] M057 Filter-before-arrange planning.
- [x] M058 Arrangement sharing.
- [x] M059 Index selection from predicate shape.
- [x] M060 View dependency graph and invalidation.
- [x] M061 Incremental view maintenance for supported typed views.
- [x] M062 Materialized view refresh and hydration state.
- [x] M063 Non-materialized SQL views.
- [ ] M064 Recursive dataflow maintenance.
- [ ] M065 Incremental window-function maintenance.
- [ ] M066 Incremental sort maintenance.
- [x] M067 Top-K with offset for supported ordered paths.
- [ ] M068 Differential group-by updates for generic SQL tables.
- [x] M068a Exact generic differential COUNT group maintenance.
- [ ] M069 Differential distinct updates for generic SQL tables.
- [x] M069a Boundary-only differential distinct maintenance.
- [ ] M070 Monotone aggregate specialization.
- [x] M070a Append-only typed-table aggregate fast path.
- [ ] M071 Late-data handling policy.
- [x] M071a Explicit accept/reject/drop late-data policy.
- [ ] M072 Watermark propagation.
- [x] M072a Monotone differential watermark propagation.
- [ ] M073 Temporal joins.
- [x] M073a Indexed weighted temporal equi-join.
- [ ] M074 Interval joins.
- [x] M074a Inclusive timestamp interval bounds for temporal joins.
- [x] M075 Temporal filtering for supported typed paths.

### Planning, Explainability, And Operations

- [x] M076 Explain plan output.
- [x] M077 Explain arrangement ownership and reuse. `TypedTableAggregateArrangements.Snapshot` and `TypedTableJoinArrangements.Snapshot` expose deterministic ownership, active lease reuse, checkpoints, source sequences, and staleness without changing execution.
- [x] M078 Explain the full dataflow graph. `BuildExplainDataflowGraph` preserves every EXPLAIN step, derives nested depth, and emits stable pipeline/subplan edges; JSON and DOT helpers are additive and read-only.
- [x] M079 Explain keys and index characteristics.
- [ ] M080 Explain optimizer notices and rejected alternatives.
- [x] M081 Arrangement and source metrics for existing structures.
- [x] M082 Operator-level row and latency metrics where instrumented.
- [ ] M083 Per-source lag and frontier metrics.
- [x] M083a Thread-safe monotone source frontier registry with deterministic lag snapshots.
- [x] M083a On-demand per-subscription frontier and lag status.
- [ ] M084 Per-operator retained-memory metrics.
- [x] M084a Thread-safe operator retained-memory gauge registry with deterministic snapshots.
- [ ] M085 Per-collection size and compaction metrics.
- [x] M085a Thread-safe collection size gauges and compaction counters with deterministic snapshots.
- [x] M086 Replica isolation for independent workloads.
- [ ] M087 Deterministic replica replay checks.
- [x] M087a Canonical ordered replay digests with sequence validation and deterministic mismatch errors.
- [ ] M088 Read replicas with explicit staleness bounds.
- [x] M088a Deterministic read-replica selection with required frontiers and maximum lag.
- [x] M089 Failover and recovery of supported replicas.
- [ ] M090 Independent compute and storage scaling.
- [x] M091 Durable persistent shards through the local storage layer.
- [x] M092 Batched writes to durable storage.
- [ ] M093 Generic persistent-shard compaction scheduling.
- [ ] M094 Persistent-shard leases and fencing.
- [x] M095 Snapshot hydration with progress reporting.
- [ ] M096 Per-column dictionary compression for arrangements.
- [x] M096a Deterministic low-cardinality string dictionary codec.
- [ ] M097 Compressed arrangement batches.
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
- [ ] T008 RTREE spatial index.
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
- [ ] T042 Recovery-time parallel replay.
- [x] T042a Recovery replay mutation fast path - scalar durable mutations avoid constructing public command responses; unsupported commands keep the existing dispatcher (see [JOURNAL_REPLAY.md](JOURNAL_REPLAY.md)).
- [x] T043 Recovery replay progress and ETA metrics.
- [x] T044 Recovery point selection by logical sequence.
- [x] T045 Crash-consistency fault injection.
- [x] T046 Online backup cancellation with resumable manifests.

### Replication And Topology

- [ ] T047 Synchronous replication with an explicit quorum.
- [x] T047a Explicit write-quorum decision helper with validation and acknowledgement reporting.
- [x] T048 Replication sets and peer topology.
- [x] T049 Vector-clock exposure for every replica - replication queue results expose an immutable observational `vector_clock` containing the local sequence and all current topology members' acknowledged sequences; it does not change quorum or conflict semantics.
- [x] T050 LSN or journal sequence exposure.
- [x] T051 Per-peer replication lag measurement: async queue status exposes source, acknowledged, and per-target lag sequences, with Prometheus gauges.
- [x] T052 Relay reconnect and retry.
- [x] T053 Bootstrap a node from a peer.
- [x] T054 Orphan replica detection and cleanup. Election status reports sorted liveness records whose node IDs are no longer in the current topology, and an authenticated `POST /api/election` with `{"cleanup_orphans":true}` prunes only those stale election records without touching cache or replica data.
- [x] T055 Idempotent journal replay.
- [ ] T056 Deterministic conflict resolution for concurrent writers.
- [x] T056a Deterministic conflict-version ordering with stable node and sequence tie-breaks.
- [x] T057 Election and failover behavior for supported topologies.
- [ ] T058 Quorum reads and writes.
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
- [ ] T076 Explicit region partition routing.
- [ ] T077 Region-local backup and restore.
- [ ] T078 Cross-region read policy.
- [ ] T079 Partition ownership and fencing metadata.
- [ ] T080 Partition split and merge tooling.
- [ ] T081 Partition-local query planning.
- [ ] T082 Partition pruning from region predicates.
- [ ] T083 Cross-partition aggregate merge.
- [ ] T084 Cross-partition ordered pagination.
- [ ] T085 Partition health and lag dashboard.

### Queues, Calls, And Runtime

- [x] T086 Queue spaces for FIFO workloads.
- [x] T087 FIFO queue operations.
- [x] T088 Priority queue operations.
- [x] T089 Delay queue operations - public generic `hatDataStructure.DelayQueue` uses a stable 4-ary deadline heap with zero steady-state allocations and `PopReady`/`NextReadyAt` operations. See [DELAY_QUEUE.md](DELAY_QUEUE.md).
- [x] T090 TTL queue expiration.
- [x] T091 Scheduled refresh and maintenance tasks.
- [ ] T092 Fiber-style cooperative scheduler.
- [x] T093 Cooperative yielding in bounded worker loops.
- [ ] T094 Channels for typed producer-consumer exchange.
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
- [ ] T105 Hot module loading with version checks.
- [x] T106 Triggers and update hooks for supported collections.
- [x] T107 Replace hooks for journal and projection maintenance.
- [ ] T108 Transactional trigger ordering guarantees.
- [x] T109 Runtime configuration with validation.
- [x] T110 Memory quotas and admission limits.
- [x] T111 Separate cache sizing from durable-storage sizing. Added independent `-cache-memory-cap-bytes` and `-db-storage-max-bytes` controls, with the old cache flag retained as a legacy alias; see the README and `BENCHMARK.md`.
- [x] T112 Per-queue memory and latency metrics - async replication exposes estimated resident queued/in-flight payload bytes plus queue wait/service histograms through status and Prometheus; see [REPLICATION_OPERATIONS.md](REPLICATION_OPERATIONS.md).
- [x] T113 Dead-letter queue with replay controls - public generic bounded retention supports inspection, explicit replay deadlines, and discard; see [DEAD_LETTER_QUEUE.md](DEAD_LETTER_QUEUE.md).
- [ ] T114 Work stealing for independent queue workers.
- [x] T115 Cancellation-safe task ownership: `HTTPReplicator.CloseWithContext` drains asynchronous work without dropping owned tasks when a shutdown deadline expires.

### SQL, Security, And Operations

- [x] T116 SQL over named collections.
- [x] T117 Explain query plan output.
- [x] T118 Parameter binding.
- [x] T119 SQL views.
- [ ] T120 SQL triggers with transaction semantics.
- [x] T121 Public SQL transaction commands. `CompileSQL` exposes `BEGIN ATOMIC` programs with savepoints, and `BeginSQLTransaction` exposes snapshot reads, rollback, conflict-aware commit, and savepoint methods.
- [x] T122 UPSERT behavior.
- [x] T123 REPLACE behavior.
- [x] T124 DELETE behavior.
- [x] T125 UPDATE behavior.
- [x] T126 RETURNING clauses. `ExecuteSQLMutation` returns selected key/value/existence/TTL columns for direct key-targeted mutations, including delete and conditional merge results.
- [x] T127 ON CONFLICT clauses. `ExecuteSQLMutation` supports primary-key `DO NOTHING` and typed `DO UPDATE ... EXCLUDED` forms with explicit rejection of unsupported expressions and expiration combinations; the supported paths are covered by tests and a five-run benchmark.
- [x] T128 MERGE statements. `ExecuteSQLMutation` supports matched and not-matched conditional `MERGE` actions through the atomic merge executor, with `RETURNING` coverage.
- [x] T129 Common table expressions.
- [ ] T130 Generated columns.
- [x] T131 Typed constraints.
- [ ] T132 Foreign-key enforcement.
- [x] T133 JSON path access.
- [ ] T134 Spatial predicates.
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
