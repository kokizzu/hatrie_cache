# Query Engine Ideas: Adoption Status

## Recently Adopted

| Source | Idea | Status | Evidence |
| ClickHouse | Filesystem cache admission | Adopted as an opt-in frequency gate for remote immutable parts | `RemotePartCacheOptions.MinAccesses` preserves eager admission at `0`, suppresses one-hit RAM retention at positive thresholds, counts concurrent single-flight accesses, and bounds candidate metadata. See [CH015_FILESYSTEM_CACHE_ADMISSION.md](CH015_FILESYSTEM_CACHE_ADMISSION.md) and [BENCHMARK.md#ch-015-filesystem-cache-admission](BENCHMARK.md#ch-015-filesystem-cache-admission). |
| Materialize | Dataflow operator placement constraints by failure domain | Adopted | Deterministic, bounded opt-in planner in `hatPipeline.PlanDataflowOperatorPlacement`; see [MZ036_DATAFLOW_OPERATOR_PLACEMENT.md](MZ036_DATAFLOW_OPERATOR_PLACEMENT.md) and [BENCHMARK.md](BENCHMARK.md#mz-036-dataflow-operator-placement). |
| Materialize | Dynamic dataflow worker scaling | Adopted | Bounded opt-in `hatPipeline.ResizablePipeline` with runtime stage resize, backpressure, and cancellation; see [MZ038_DYNAMIC_DATAFLOW_WORKER_SCALING.md](MZ038_DYNAMIC_DATAFLOW_WORKER_SCALING.md) and [BENCHMARK.md](BENCHMARK.md#mz-038-dynamic-dataflow-worker-scaling). |
| Materialize | Costed dataflow explanation | Adopted as an explicit opt-in SQL explain mode | `EXPLAIN COST` adds bounded heuristic operator CPU and memory estimates without reading sources or changing ordinary execution; see [MZ044_COSTED_EXPLAIN.md](MZ044_COSTED_EXPLAIN.md) and [BENCHMARK.md](BENCHMARK.md#mz-044-costed-sql-explain). |
| ClickHouse | Parallel format parsing | Partially adopted as an opt-in NDJSON parser | `hatSql.ParseNDJSONParallel` decodes independent records concurrently, preserves source order, and reports the lowest invalid line deterministically; CSV and default streaming paths remain unchanged. See [CH047_PARALLEL_FORMAT_PARSING.md](CH047_PARALLEL_FORMAT_PARSING.md) and [BENCHMARK.md](BENCHMARK.md#ch-047-parallel-ndjson-format-parsing). |
| ClickHouse | Adaptive low-cardinality dictionaries | Adopted as an opt-in typed-table representation | `TypedTableColumn.DictionaryAdaptive` admits bounded low-cardinality strings and demotes promoted columns once active cardinality exceeds 32; static dictionary and default plain paths remain unchanged. See [CHU16_ADAPTIVE_LOW_CARDINALITY.md](CHU16_ADAPTIVE_LOW_CARDINALITY.md) and [BENCHMARK.md](BENCHMARK.md#chu16-adaptive-low-cardinality-string-storage). |
| --- | --- | --- | --- |
| ClickHouse | Mutation wait/status lifecycle | Adopted for asynchronous journal writes | `CommandJournalSubmission` exposes the durable journal sequence, committed/rejected/failed state, and infrastructure error; `CommandJournal.MutationStatus` rereads committed state after restart, while `system.mutations` adds privacy-safe `mutation_id` and `progress`. [CHU07_MUTATION_LIFECYCLE.md](CHU07_MUTATION_LIFECYCLE.md) |
| ClickHouse | Mutation dependency graph with resumable progress | Adopted as an importable caller-driven maintenance coordinator | `hatPipeline.MutationDependencyGraph` maintains reverse dependents and a deterministic ready set, blocks dependents after failures, supports explicit retry, and restores running work as pending. It does not execute or persist user work automatically. See [MUTATION_DEPENDENCY_GRAPH.md](MUTATION_DEPENDENCY_GRAPH.md) and [BENCHMARK.md#mutation-dependency-graph](BENCHMARK.md#mutation-dependency-graph). |
| ClickHouse | Projection refresh lag and failure state | Adopted as a bounded incremental-projection status snapshot | `IncrementalProjectionRunner.Status` reports applied/observed sequence frontiers, lag, refresh state, bounded last error, timestamps, and consecutive failures without changing durable checkpoint semantics; the runner remains disabled by default. See [CH018_PROJECTION_REFRESH_STATUS.md](CH018_PROJECTION_REFRESH_STATUS.md) and [BENCHMARK.md#ch-018-projection-refresh-lag-and-failure-state](BENCHMARK.md#ch-018-projection-refresh-lag-and-failure-state). |
| Materialize | Worker scaling coordination | Adopted as an opt-in resizable local task scheduler | `hatPipeline.ResizableScheduler` changes worker targets online, never preempts callbacks, preserves queued tasks, drains gracefully, and keeps the existing fixed `Scheduler` path unchanged. See [MZ020_RESIZABLE_SCHEDULER.md](MZ020_RESIZABLE_SCHEDULER.md) and [BENCHMARK.md#mz-020-resizable-scheduler](BENCHMARK.md#mz-020-resizable-scheduler). |
| Tarantool | Tuple-level compression | Adopted as an importable opt-in bounded codec | `hatDataStructure.TupleCompressor` emits self-describing `HTC1` frames with adaptive ZSTD admission, raw fallback, reusable scratch storage, CRC32 validation, and pre-decode size limits; existing persistence and peer-wire defaults remain unchanged. See [TT045_TUPLE_COMPRESSION.md](TT045_TUPLE_COMPRESSION.md) and [BENCHMARK.md#tt-045-tuple-level-compression](BENCHMARK.md#tt-045-tuple-level-compression). |
| Tarantool | Functional indexes over deterministic expressions | Partially adopted for materialized SQL sources | `hatSchema.MaterializedSource.BuildFunctionalIndex` publishes an opt-in generation-checked equality index, maintains later inserts, and lets `LOWER(field)` use the existing SQL resolver hook. Generic typed functional indexes remain caller-managed; expression registration and arbitrary planner inference stay deferred. See [TR023_FUNCTIONAL_INDEX.md](TR023_FUNCTIONAL_INDEX.md) and [BENCHMARK.md#tr-023-functional-indexes](BENCHMARK.md#tr-023-functional-indexes). |
| Tarantool | Index selectivity and distribution statistics | Partially adopted for materialized SQL sources | `hatSchema.MaterializedSource.IndexStats` caches bounded posting distributions, while `IndexValueEstimate` supplies exact point cardinality. The existing planner uses these values to choose the smallest competing equality index; single-index probes keep the direct fast path. See [TR030_INDEX_STATS.md](TR030_INDEX_STATS.md) and [BENCHMARK.md#tr-030-materialized-index-statistics](BENCHMARK.md#tr-030-materialized-index-statistics). |
| Materialize | Public since and upper read holds | Adopted as a typed-table changefeed retention guard | `TypedTable.AcquireChangeReadHold` snapshots an inclusive upper frontier, caps multi-call `ChangesAfter` reads, supports monotone advancement and idempotent release, and blocks `CompactChangesThrough` while a reader can still need the history. The hold is in-memory and process-local; distributed leases and restart recovery remain caller-owned. See [MZ002_TYPED_TABLE_READ_HOLDS.md](MZ002_TYPED_TABLE_READ_HOLDS.md) and [BENCHMARK.md#mz-002-typedtable-change-read-holds](BENCHMARK.md#mz-002-typedtable-change-read-holds). |
| ClickHouse / Materialize | Mergeable Count-Min Sketch partial state | Adopted as an importable bounded aggregate-state primitive | `hatCache.CountMinSketch` exposes validated portable snapshots, same-shape pointwise saturating merge, zero-value receiver adoption, and `HatTrie.MergeCountMinSketch`; ordinary commands and formats remain unchanged. See [BENCHMARK.md#ch-060-mergeable-count-min-sketch-partial-state](BENCHMARK.md#ch-060-mergeable-count-min-sketch-partial-state). |
| ClickHouse / Materialize | Versioned portable partial aggregate envelope | Adopted as an importable compact transfer format | `hatDataStructure.AggregateStateEnvelope` provides bounded HAG1 framing, explicit kind/version metadata, strict length checks, and CRC32 corruption detection; `HyperLogLog` and `CountMinSketch` write/read raw register or counter payloads without changing existing command, journal, or storage defaults. See [BENCHMARK.md#ch-061-versioned-partial-aggregate-envelope](BENCHMARK.md#ch-061-versioned-partial-aggregate-envelope). |
| ClickHouse / Materialize | Mergeable approximate top-K state | Adopted as an importable bounded sketch merge | `hatCache.TopK.Merge` unions partition summaries with deterministic ranking, `MarshalAggregateState`/`NewTopKFromAggregateState` provide HAG1 transfer, and `HatTrie.MergeTopK` replaces or merges a typed-trie value without changing command defaults. See [BENCHMARK.md#ch-062-mergeable-approximate-top-k-state](BENCHMARK.md#ch-062-mergeable-approximate-top-k-state). |
| ClickHouse / Materialize | Mergeable quantile aggregate state | Adopted as a compact TDigest transfer format | `hatDataStructure.TDigest` retains its existing merge/estimate behavior while `MarshalAggregateState`, `NewTDigestFromAggregateState`, and `MergeAggregateState` add fixed-width centroid transfer with HAG1 validation and CRC protection. Existing commands and defaults remain unchanged. See [BENCHMARK.md#ch-063-compact-mergeable-tdigest-aggregate-state](BENCHMARK.md#ch-063-compact-mergeable-tdigest-aggregate-state). |
| Tarantool | Point-in-time snapshot restore by journal sequence | Adopted as an opt-in recovery boundary | `BackupBundleRestoreOptions.MaxJournalSequence` restores snapshot bundles through an exact committed sequence, supports JSON and binary command journals, and preserves the complete restore path at the default `0`. Repository/Pebble checkpoint restore remains intentionally unsupported. See [TT011_POINT_IN_TIME_RESTORE.md](TT011_POINT_IN_TIME_RESTORE.md) and [BENCHMARK.md#tt-011-point-in-time-snapshot-restore](BENCHMARK.md#tt-011-point-in-time-snapshot-restore). |
| Tarantool | Tuple field-offset cache | Adopted as an opt-in columnar lookup cache | `ColumnarBatch.PrepareFieldOffsets` resolves repeated logical-field reads through one schema-aware offset map and typed physical slots, while mutation and repacking invalidate the sidecar. `TypedTableColumnarCacheOptions.FieldOffsetCache` enables it only for reused cached layouts; the default remains unchanged. See [TR019_TUPLE_FIELD_OFFSETS.md](TR019_TUPLE_FIELD_OFFSETS.md) and [BENCHMARK.md#tr-019-tuple-field-offset-cache](BENCHMARK.md#tr-019-tuple-field-offset-cache). |
| Tarantool | Durable tuple field-operation journal | Adopted as an opt-in bounded replay boundary | `hatDataStructure.TupleFieldOperationJournal` records atomic `SET`/`SPLICE`/`ADD` batches with retained-operation idempotency, CRC32C/version checks, atomic `0600` persistence, compaction-gap detection, and failure-atomic replay. Direct tuple mutation and application-level tuple association remain unchanged. See [TU19_DURABLE_TUPLE_OPERATION_JOURNAL.md](TU19_DURABLE_TUPLE_OPERATION_JOURNAL.md) and [BENCHMARK.md#t-u19-durable-tuple-field-operation-journal](BENCHMARK.md#t-u19-durable-tuple-field-operation-journal). |
| Materialize | Incremental Top-K maintenance | Partially adopted as an importable exact weighted differential operator | `hatSql.IncrementalTopK` maintains keyed rows with signed `int64` multiplicities, SQL ordering, deterministic key ties, atomic batches, and transition-only output. It requires a stable `DifferentialRow.Key`; automatic SQL plan integration and distributed exchange remain deferred. See [MZ037_INCREMENTAL_TOP_K.md](MZ037_INCREMENTAL_TOP_K.md) and [BENCHMARK.md#mz-037-incremental-weighted-top-k](BENCHMARK.md#mz-037-incremental-weighted-top-k). |
| Materialize | General incremental distinct | Partially adopted as an importable stateful differential operator | `hatSql.IncrementalDistinct` retains keyed multiplicities across batches, emits only `0 -> positive` and `positive -> 0` transitions, validates atomically, and returns deterministic cloned snapshots. Automatic SQL plan integration remains deferred. See [MZ039_INCREMENTAL_DISTINCT.md](MZ039_INCREMENTAL_DISTINCT.md) and [BENCHMARK.md#mz-039-incremental-distinct](BENCHMARK.md#mz-039-incremental-distinct). |
| Materialize | Incremental percentile | Partially adopted as an importable exact weighted order-statistics operator | `hatSql.IncrementalPercentile` maintains keyed multiplicities in a SQL-ordered treap, applies signed batches atomically, and answers nearest-rank percentile reads without a full relation sort. Automatic SQL plan integration, distributed arrangements, and approximate mergeable sketches remain deferred. See [MZ040_INCREMENTAL_PERCENTILE.md](MZ040_INCREMENTAL_PERCENTILE.md) and [BENCHMARK.md#mz-040-incremental-percentile](BENCHMARK.md#mz-040-incremental-percentile). |
| Materialize | Generic negative-diff operators | Partially adopted as an importable differential operator API | `hatSql.DifferentialRow` carries signed `int64` weights through filter, map, flat-map, union, join, difference, intersection, and grouped aggregate APIs with duplicate preservation, overflow validation, and atomic stateful updates. Automatic SQL plan wiring and multiset coverage across every SQL operator remain deferred. See [MZ034_GENERIC_NEGATIVE_DIFF.md](MZ034_GENERIC_NEGATIVE_DIFF.md) and [BENCHMARK.md#mz-034-generic-negative-diff-operators](BENCHMARK.md#mz-034-generic-negative-diff-operators). |
| ClickHouse | Refreshable external dictionaries | Adopted as an opt-in SQL function registry | `SQLExternalDictionary` publishes immutable snapshots atomically, retains the last good snapshot across bounded refresh failures, supports `DICT_GET`, `DICT_GET_OR_DEFAULT`, and `DICT_HAS`, and keeps per-lookup counters disabled by default. See [SQL_EXTERNAL_DICTIONARIES.md](SQL_EXTERNAL_DICTIONARIES.md) and [BENCHMARK.md#ch-049-refreshable-external-dictionaries](BENCHMARK.md#ch-049-refreshable-external-dictionaries). |
| ClickHouse | Query profiler samples | Adopted as an opt-in bounded API | `SQLQueryProfiler` stores sampled operator CPU/blocking/row/byte observations by query ID, evicts old query IDs and samples at configured bounds, and leaves ordinary SQL execution unchanged unless callers explicitly record samples. See [SQL_QUERY_PROFILER.md](SQL_QUERY_PROFILER.md) and [BENCHMARK.md#ch-032-query-profiler-samples](BENCHMARK.md#ch-032-query-profiler-samples). |
| ClickHouse | Per-part and per-column query profiling | Adopted as an opt-in bounded aggregate API | `SQLQueryProfiler.RecordPartColumn` aggregates caller-supplied read/write CPU, row, and byte observations by query, physical part, and column with deterministic snapshots, saturation, and explicit dropped-observation counters. See [CH235_PART_COLUMN_PROFILER.md](CH235_PART_COLUMN_PROFILER.md) and [BENCHMARK.md#c235-part-column-query-profiling](BENCHMARK.md#c235-part-column-query-profiling). |
| ClickHouse | Adaptive integer delta and Gorilla float encoding | Adopted as opt-in `hatDataStructure` codecs | Monotone `uint64` values use first-value-plus-varint deltas, while repetitive `float64` values use exact XOR bit windows; raw fallback prevents expansion. Decode CPU is higher, so existing persistence and replication defaults remain unchanged. See [C247_UINT64_DELTA_CODEC.md](C247_UINT64_DELTA_CODEC.md). |
| ClickHouse | Vertical TTL deletion using only deletion masks and narrow key/expiry columns | Adopted as an opt-in `hatDataStructure.PersistentDeleteBitmap` operation | `ApplyVerticalTTLDeletes` scans live bitmap words without reading payload columns; `ApplyVerticalTTLDeletesInto` reuses caller storage for zero-allocation maintenance passes. See [C245_VERTICAL_TTL_DELETE.md](C245_VERTICAL_TTL_DELETE.md) and [BENCHMARK.md](BENCHMARK.md#c245-vertical-ttl-deletion). |
| ClickHouse | TTL-driven recompression separate from row deletion | Adopted as an opt-in tuple-frame lifecycle policy | `TTLRecompressionPolicy` selects keep, rewrite, or delete independently; `TupleCompressor.RecompressIfDue` avoids decoding on keep/delete and rewrites with a caller-selected colder compressor. Existing storage and TTL defaults remain unchanged. See [C246_TTL_RECOMPRESSION.md](C246_TTL_RECOMPRESSION.md) and [BENCHMARK.md](BENCHMARK.md#c246-ttl-driven-recompression). |
| Kafka | Read-only changefeed offset inspection | Adopted as a bounded `hatReplication.SpaceChangefeed.Inspect` API | Consumers can inspect retained events after an exclusive sequence without creating a subscriber or advancing a checkpoint. History gaps, future offsets, response events, and payload bytes are bounded and validated. See [C249_OFFSET_INSPECTION.md](C249_OFFSET_INSPECTION.md). |
| ClickHouse | Probabilistic query-log sampling | Adopted as an opt-in deterministic durable-log sampler | `SQLQueryLogOptions.SampleRate` and `SampleSeed` retain a bounded Bernoulli sample of valid privacy-safe records, expose observed/accepted/dropped counters, and skip rotation and writes for dropped entries; zero preserves the prior retain-all default. Sampling intentionally occurs after validation and JSON encoding. See [CHU38_QUERY_LOG_SAMPLING.md](CHU38_QUERY_LOG_SAMPLING.md) and [BENCHMARK.md#ch-u38-sampled-query-log-export](BENCHMARK.md#ch-u38-sampled-query-log-export). |
| ClickHouse | Named settings collections and inheritable validated profiles | Adopted as an imported bounded registry | `SQLNamedSettingsRegistry` publishes immutable versioned profiles atomically, supports revision compare-and-swap updates, optional parent inheritance with cycle/depth/effective-size checks, caller-supplied setting validation, isolated `Resolve`/`Lookup` reads, and allocation-free single-value lookup without changing SQL defaults. See [SQL_NAMED_SETTINGS.md](SQL_NAMED_SETTINGS.md), [INSPIRATION_BACKLOG.md](INSPIRATION_BACKLOG.md#clickhouse-50-candidates), and [BENCHMARK.md#ch-001-named-settings-profile-inheritance-and-validation](BENCHMARK.md#ch-001-named-settings-profile-inheritance-and-validation). |
| ClickHouse | Soft and hard namespace admission profiles | Adopted as an opt-in static policy preview | `NamespaceResourceProfile` separates advisory soft limits from existing hard limits, and `NamespaceQueryGovernor.DryRun` reports effective hard clamps and soft warnings without reserving queues, quotas, or compute work. See [SQL_NAMESPACE_ADMISSION.md](SQL_NAMESPACE_ADMISSION.md) and [BENCHMARK.md#ch-003-soft-and-hard-namespace-admission-profiles](BENCHMARK.md#ch-003-soft-and-hard-namespace-admission-profiles). |
| ClickHouse | Retained query-log tables with time and size rotation | Adopted as an opt-in bounded diagnostic log | `SQLQueryLogOptions` can rotate privacy-safe query history by active-file bytes or age, retain numbered archives, and read retained segments oldest-first; zero rotation settings preserve the append-only default. See [SQL_QUERY_LOG.md](SQL_QUERY_LOG.md) and [BENCHMARK.md#ch-004-retained-query-log-rotation](BENCHMARK.md#ch-004-retained-query-log-rotation). |
| ClickHouse | Retained part and merge event log | Adopted as an opt-in bounded typed-table lifecycle log | `TypedTableStorageEventLogOptions` retains bounded base-part, deferred-delete patch-part, and patch-merge events with physical row counts, pending deletes, table/event sequences, and merge duration; the default is disabled and no keys or values are retained. See [TYPED_TABLE_STORAGE_EVENTS.md](TYPED_TABLE_STORAGE_EVENTS.md) and [BENCHMARK.md#ch-005-typed-table-storage-events](BENCHMARK.md#ch-005-typed-table-storage-events). |
| ClickHouse | LRU sparse-mark cache separate from data-part cache | Adopted as an opt-in typed-table metadata cache | `TypedTableColumnarCacheOptions.SparsePrimaryMarkCache` retains only ordered primary-field bounds under a separate byte-bounded LRU, reuses them after full batch eviction, and clears them on mutation; default is disabled. See [TYPED_TABLE_SPARSE_MARK_CACHE.md](TYPED_TABLE_SPARSE_MARK_CACHE.md) and [BENCHMARK.md#ch-006-lru-sparse-primary-mark-cache](BENCHMARK.md#ch-006-lru-sparse-primary-mark-cache). |
| ClickHouse | Composite primary marks | Adopted as an opt-in flat tuple-mark path | `TypedTableColumnarCacheOptions.SparsePrimaryFields` stores first/last tuples for up to eight ordered numeric fields and prunes leading-prefix equality/range predicates without changing wire or persistence formats; incomplete or unsorted batches fall back safely and the existing single-field option remains compatible. See [CHU18_COMPOSITE_PRIMARY_MARK_PRUNING.md](CHU18_COMPOSITE_PRIMARY_MARK_PRUNING.md) and [BENCHMARK.md#ch-u18-composite-primary-mark-pruning](BENCHMARK.md#ch-u18-composite-primary-mark-pruning). |
| ClickHouse | General `PREWHERE` syntax | Adopted as a public filter-stage contract | `PREWHERE` is parsed, bound, executed before `WHERE` on streamable sources, combined safely for fallback execution, represented in `EXPLAIN`, and covered by NULL/parameter/late-materialization tests; unsupported specialized plans retain the established executor. |
| ClickHouse | Streaming `JSONEachRow` and CSV ingestion | Adopted as bounded external-table reader and callback APIs | `ExternalImportOptions` bounds rows and bytes, JSONEachRow lines, CSV/JSON callbacks, and atomic reader imports; legacy byte-slice imports remain unchanged. See [CHU21_STREAMING_TEXT_INGESTION.md](CHU21_STREAMING_TEXT_INGESTION.md) and [BENCHMARK.md#ch-u21-streaming-text-ingestion](BENCHMARK.md#ch-u21-streaming-text-ingestion). |
| ClickHouse | Decompressed column block cache with admission control | Adopted as an opt-in typed-table decode cache | `TypedTableColumnarCacheOptions.DecompressedBlockCache` retains bounded decoded scalar blocks behind per-column atomic read slots, admits blocks after repeated misses, and clears them with the immutable layout cache; it requires `CompressedBatches` and defaults off. See [TYPED_TABLE_DECOMPRESSED_BLOCK_CACHE.md](TYPED_TABLE_DECOMPRESSED_BLOCK_CACHE.md) and [BENCHMARK.md#ch-007-decompressed-column-block-cache-with-admission](BENCHMARK.md#ch-007-decompressed-column-block-cache-with-admission). |
| Materialize | Stale-read rejection | Adopted as an opt-in source frontier requirement | `SQLQueryOptions.RequireSourceFrontier` checks every non-local source through `SQLSourceFrontierResolver` before reads and rejects unavailable, unready, or stale sources across joins, CTEs, and set-operation branches. See [SQL_SOURCE_FRONTIERS.md](SQL_SOURCE_FRONTIERS.md) and [BENCHMARK.md#mz-007-source-frontier-requirement](BENCHMARK.md#mz-007-source-frontier-requirement). |
| Materialize | Frontier-aware source backpressure | Adopted as an opt-in source admission gate | `hatPipeline.FrontierBackpressure` bounds producer progress in a caller-selected frontier domain, supports nonblocking rejection or cancellation-aware waiting, wakes on monotone consumer advancement and close, and retains no source records. Automatic connector wiring and distributed coordination remain caller-owned. See [MZ007_FRONTIER_SOURCE_BACKPRESSURE.md](MZ007_FRONTIER_SOURCE_BACKPRESSURE.md) and [BENCHMARK.md#mz-07-frontier-aware-source-backpressure](BENCHMARK.md#mz-07-frontier-aware-source-backpressure). |
| Materialize | Historical `AS OF` reads | Adopted as an opt-in exact-frontier snapshot contract | `SQLQueryOptions.AsOfFrontier` asks `SQLFrontierSnapshotProvider` for an immutable historical resolver across normal, streamed, offset-page, and keyset-page execution, bypasses live result-cache entries, and binds cursors to the frontier. See [SQL_AS_OF.md](SQL_AS_OF.md) and [BENCHMARK.md#mz-008-sql-as-of-historical-reads](BENCHMARK.md#mz-008-sql-as-of-historical-reads). |
| Materialize | Temporal validity filters | Partially adopted as a native SQL predicate | `VALID_AT(at, valid_from, valid_to)` implements half-open `[valid_from, valid_to)` intervals with `NULL` open bounds and stays on the native scalar path for literal/field arguments. Physical validity indexes and frontier-aware pruning remain deferred. See [SQL_TEMPORAL_VALIDITY.md](SQL_TEMPORAL_VALIDITY.md) and [BENCHMARK.md#mz-009-temporal-validity-filters](BENCHMARK.md#mz-009-temporal-validity-filters). |
| Materialize | Frontier-aware logical compaction | Partially adopted as an opt-in maintenance admission layer | `hatPipeline.FrontierCompactionScheduler` and `FrontierRetentionRegistry.WaitUntilSafe` wait for a safe lower frontier and released historical-read leases before queueing caller-owned compaction work. Durable jobs, priorities, coalescing, and automatic SQL integration remain open. See [MZ003_FRONTIER_COMPACTION_SCHEDULER.md](MZ003_FRONTIER_COMPACTION_SCHEDULER.md) and [BENCHMARK.md#mz-003-frontier-aware-compaction-admission](BENCHMARK.md#mz-003-frontier-aware-compaction-admission). |
| Materialize | `TAIL`/`SUBSCRIBE` changefeed | Partially adopted as opt-in journal and SQL-result subscriptions | `CommandJournal.Subscribe` handles durable command tails; `QuerySubscriptions.SubscribeSQL` and `SubscribeDifferentialSQL` derive static `CACHE(...)` dependencies and reuse bounded snapshot/differential delivery. SQL statement grammar and signed wire envelopes remain open. See [MZ010_JOURNAL_SUBSCRIPTIONS.md](MZ010_JOURNAL_SUBSCRIPTIONS.md), [MZ010_SQL_SUBSCRIPTIONS.md](MZ010_SQL_SUBSCRIPTIONS.md), and [BENCHMARK.md#mz-010-sql-result-subscription-entrypoints](BENCHMARK.md#mz-010-sql-result-subscription-entrypoints). |
| Tarantool | Key-change notification watchers | Adopted as opt-in journal key watchers | `CommandJournalSubscribeOptions.KeyPrefix` filters replay/live records by literal key prefix, while `Coalesce` bounds pending live state to the configured number of unique keys and keeps the newest record per key in sequence order. See [TT040_SPACE_CHANGEFEED.md](TT040_SPACE_CHANGEFEED.md) and [BENCHMARK.md#t-g42-key-watchers](BENCHMARK.md#t-g42-key-watchers). |
| Tarantool | Space/key-range replication filters | Adopted as an opt-in literal key-prefix replication filter | `HTTPReplicatorOptions.ReplicationKeyPrefixes` filters live command fan-out and digest anti-entropy, carries the bounded scope to peers, bypasses whole-dataset Merkle sync, and preserves out-of-scope peer keys even with older digest peers. See [TR004_REPLICATION_KEY_FILTER.md](TR004_REPLICATION_KEY_FILTER.md) and [BENCHMARK.md#tr-04-replication-key-prefix-filter](BENCHMARK.md#tr-04-replication-key-prefix-filter). |
| Materialize | Sink connector API | Partially adopted as an importable at-least-once command-journal sink runner | `CommandJournalSink` delivers ordered bounded batches with optional durable sequence checkpoints, cancellation, and explicit failure propagation. Concrete Kafka/file/HTTP adapters and exactly-once sink transactions remain connector-owned or open. See [MZ011_SINK_CONNECTORS.md](MZ011_SINK_CONNECTORS.md) and [BENCHMARK.md#mz-011-sink-connectors](BENCHMARK.md#mz-011-sink-connectors). |
| Materialize | Exactly-once sink checkpoints | Partially adopted as a sink-owned transactional runner | `CommandJournalExactlyOnceSink` loads the committed watermark, stages batches, commits output and sequence together, rolls back pre-commit failures, and stops on ambiguous commit errors without retrying. See [MZ012_EXACTLY_ONCE_SINK.md](MZ012_EXACTLY_ONCE_SINK.md) and [BENCHMARK.md#mz-012-exactly-once-sink-checkpoints](BENCHMARK.md#mz-012-exactly-once-sink-checkpoints). |
| Materialize | Source connector checkpoints | Partially adopted as an opt-in journal-bound source offset API | `CommandJournalSourceCheckpointCoordinator` loads and durably saves opaque binary source offsets with the latest fully applied journal sequence, rejects checkpoints ahead of local recovery, and preserves at-least-once replay when saving fails. Durable store transactions and source-specific polling remain connector-owned. See [MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md](MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md) and [BENCHMARK.md#mz-013-source-connector-checkpoints](BENCHMARK.md#mz-013-source-connector-checkpoints). |
| Materialize | Upsert-source consolidation | Partially adopted as an importable source-batch primitive | `hatDataStructure.UpsertBatch[T]` collapses repeated keyed updates, retains delete tombstones, preserves first-seen order, and reuses capacity across batches. Automatic connector polling, flush policy, and SQL DML integration remain caller-owned. See [MZ014_UPSERT_BATCH.md](MZ014_UPSERT_BATCH.md) and [BENCHMARK.md#mz-014-upsert-batch-consolidation](BENCHMARK.md#mz-014-upsert-batch-consolidation). |
| Materialize | Keyed upsert envelopes | Adopted as an importable current-image change boundary | `hatSql.UpsertChangefeed` exposes stable key columns, detached current row images, deletion tombstones, and source frontier metadata without retaining before images. It intentionally requires unit multiplicity; use the Debezium adapter or differential subscription when before images or counts are required. See [M206_UPSERT_ENVELOPES.md](M206_UPSERT_ENVELOPES.md) and [BENCHMARK.md#mz-016-keyed-upsert-envelopes](BENCHMARK.md#mz-016-keyed-upsert-envelopes). |
| Materialize | CDC envelope normalization | Partially adopted as an importable dynamic change boundary | `hatSql.NormalizeCDCEnvelope` validates keyed insert/update/delete shapes, accepts common source aliases including Debezium snapshots and Tarantool replace/upsert events, and returns an allocation-free canonical change. Connectors still own source-specific decoding, offsets, transaction IDs, and differential `diff` mapping. See [CDC_ENVELOPES.md](CDC_ENVELOPES.md) and [BENCHMARK.md#mz-015-cdc-envelope-normalization](BENCHMARK.md#mz-015-cdc-envelope-normalization). |
| Materialize | Parallel source hydration | Partially adopted as a bounded local-partition restore pool | Partitioned snapshot and Pebble hydration dispatch each key to its deterministic local partition and support `HatTrie.ConfigureSnapshotRestoreWorkers`; `0` keeps automatic `GOMAXPROCS` sizing, positive values cap queue workers, and non-partitioned restore remains serial. See [SNAPSHOT_RESTORE_WORKERS.md](SNAPSHOT_RESTORE_WORKERS.md) and [BENCHMARK.md#mz-017-bounded-partition-restore-workers](BENCHMARK.md#mz-017-bounded-partition-restore-workers). |
| Materialize | Compute/storage separation | Partially adopted as an opt-in SQL compute admission pool | `SQLQueryManagerOptions.ComputeWorkers` and `ComputeQueueCapacity` run managed queries on a bounded independent `hatPipeline` pool; `0` preserves the legacy path, `Close` drains admitted work, and durable storage or distributed compute remain caller-owned. See [SQL_COMPUTE_STORAGE_SEPARATION.md](SQL_COMPUTE_STORAGE_SEPARATION.md) and [BENCHMARK.md#mz-018-optional-sql-compute-pool](BENCHMARK.md#mz-018-optional-sql-compute-pool). |
| Materialize | Resolver-only compute adapters | Partially adopted as an opt-in stateless SQL registration path | `hatStorage.SQLResolverAdapter` lets a compute process execute through a caller-owned resolver without a local `hatStorage.Engine`; local `SQLNamespaceAdapter` validation and default execution remain unchanged. Remote transport, snapshot consistency, and failure policy remain caller-owned. See [SQL_COMPUTE_STORAGE_SEPARATION.md](SQL_COMPUTE_STORAGE_SEPARATION.md) and [BENCHMARK.md#m090a-resolver-only-sql-compute-adapter](BENCHMARK.md#m090a-resolver-only-sql-compute-adapter). |
| Materialize | Context-aware remote source cancellation | Partially adopted as an optional materialized SQL resolver contract | `hatSql.ContextSourceResolver` receives the query context for cache/keys materialization, native dataflow, and explain snapshots; `SQLSession` and `CatalogResolver` forward it, while legacy `SourceResolver` implementations remain unchanged. See [SQL_COMPUTE_STORAGE_SEPARATION.md](SQL_COMPUTE_STORAGE_SEPARATION.md) and [BENCHMARK.md#m090b-context-aware-materialized-source-resolver](BENCHMARK.md#m090b-context-aware-materialized-source-resolver). |
| Materialize | Per-cluster resource isolation | Partially adopted as opt-in named namespace compute pools | `NamespaceQueryGovernor` can give each namespace its own bounded `hatPipeline` worker and queue budget; `ComputeWorkers: 0` keeps the existing caller-goroutine path, and `Close` drains admitted work. Process-wide RSS caps and online resizing remain deferred. See [SQL_NAMESPACE_COMPUTE_POOLS.md](SQL_NAMESPACE_COMPUTE_POOLS.md) and [BENCHMARK.md#mz-019-named-sql-compute-pools](BENCHMARK.md#mz-019-named-sql-compute-pools). |
| Materialize | Index hydration readiness | Partially adopted as a synchronous SQL JSON index readiness barrier | `HatTrie.WaitSQLJSONIndexReady` returns an existing current index immediately or schedules and runs a cooperative rebuild under the caller context, making startup and deployment checks deterministic without polling. See [SQL_JSON_INDEX_READINESS.md](SQL_JSON_INDEX_READINESS.md) and [BENCHMARK.md#mz-022-sql-json-index-readiness](BENCHMARK.md#mz-022-sql-json-index-readiness). |
| Materialize | Resumable `TAIL` cursor tokens | Adopted as an opt-in HTTP journal cursor | `GET /api/journal` can return a bounded HMAC cursor bound to the journal path and schema version; `cursor` resumes after the last delivered sequence, while `after_sequence` remains the default-compatible path and binary payloads keep their existing format. See [MZ024_JOURNAL_CURSOR.md](MZ024_JOURNAL_CURSOR.md) and [BENCHMARK.md#mz-024-signed-journal-tail-cursors](BENCHMARK.md#mz-024-signed-journal-tail-cursors). |
| ClickHouse | Row TTL | Adopted as an opt-in typed-table policy with background maintenance | `TypedTableTTLOptions` supports processing-time and event-time expiry, immediate visibility filtering, an indexed `PurgeExpired` path, explicit DELETE changes, and patch-compaction-safe deadline movement. `TypedTableTTLScheduler` adds one shared explicit reaper with bounded cycles and status; `MarshalTTLState`/`RestoreTTLState` add atomic CRC-protected processing-time deadline recovery. The default remains disabled; column-level TTL and automatic schema integration remain caller-owned. See [CH007_ROW_TTL.md](CH007_ROW_TTL.md), [CH007_TTL_SCHEDULER.md](CH007_TTL_SCHEDULER.md), and [BENCHMARK.md#ch-007-background-ttl-scheduler](BENCHMARK.md#ch-007-background-ttl-scheduler). |
| ClickHouse | Incremental part backup | Adopted as content-addressed object storage with optional durable chain catalog | `ObjectStoreLayoutAuto` selects content-addressed payloads for incremental Pebble backups, deduplicates duplicate and previously present objects, records changed hashes in the manifest, restores through verified hash-derived keys, and can append successful incremental manifests to `BackupManifestCatalog`. The path layout remains configurable; retention execution remains caller-managed. See [CH022_INCREMENTAL_PART_BACKUP.md](CH022_INCREMENTAL_PART_BACKUP.md) and [BENCHMARK.md#ch-022-incremental-part-backup](BENCHMARK.md#ch-022-incremental-part-backup). |
| ClickHouse | Part/WAL-consistent backup manifest | Adopted as an additive backup integrity contract | `hatBackup.BundleConsistency` binds sorted immutable snapshot/storage/metadata/payload files and the persisted journal boundary with size and SHA-256 checks, distinguishes immutable `PartSequence` from a later `JournalSequence`, validates legacy manifests, and protects bundle/repository reads. See [CHU50_PART_WAL_CONSISTENCY.md](CHU50_PART_WAL_CONSISTENCY.md) and [BENCHMARK.md#ch-u50-partwal-consistent-backup-manifest](BENCHMARK.md#ch-u50-partwal-consistent-backup-manifest). |
| ClickHouse | External aggregation spill | Partially adopted as an opt-in bounded SQL executor path | Single-field direct `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` grouping over streamable `CACHE` and `VALUES` sources can spill and merge under `MaxGroupBytes`; unordered queries are covered, while richer SQL shapes retain their existing fallback. See [CHG01_EXTERNAL_GROUP_SPILL.md](CHG01_EXTERNAL_GROUP_SPILL.md) and [BENCHMARK.md#ch-g01-bounded-external-group-by-aggregation-spill](BENCHMARK.md#ch-g01-bounded-external-group-by-aggregation-spill). |
| Materialize | Verified safe-frontier blob garbage collection | Partially adopted as an opt-in content-addressed retention executor | `hatBackup.ObjectStoreTarget` lists physical `objects/` keys, verifies a complete `PlanBackupRetention` chain and every retained object, skips unknown names, and applies only the reviewed deletion list. Distributed frontier leases, concurrent-writer coordination, and atomic multi-object delete remain open. See [MZ006_OBJECT_STORE_GARBAGE_COLLECTION.md](MZ006_OBJECT_STORE_GARBAGE_COLLECTION.md) and [BENCHMARK.md#mz-006-verified-object-store-garbage-collection](BENCHMARK.md#mz-006-verified-object-store-garbage-collection). |
| ClickHouse | External dictionary cache with bounded refresh | Adopted as a public bounded dictionary package | `hat/hatDictionary` provides batch refresh limits, TTLs, approximate-LRU entry/byte bounds, stale-on-source-error opt-in, and zero-allocation read hits. See [CH027_EXTERNAL_DICTIONARY_CACHE.md](CH027_EXTERNAL_DICTIONARY_CACHE.md) and [BENCHMARK.md#ch-027-external-dictionary-cache](BENCHMARK.md#ch-027-external-dictionary-cache). |
| ClickHouse | Dictionary version and fallback semantics | Adopted as optional version-pinned reads and explicit fallback | `VersionedSource`, `RefreshAtVersion`, and `LookupAtVersion` reject mismatched snapshots; fallback-on-miss and fallback-on-error are opt-in and expose provenance in results. See [CH028_DICTIONARY_VERSION_FALLBACK.md](CH028_DICTIONARY_VERSION_FALLBACK.md) and [BENCHMARK.md#ch-028-dictionary-version-and-fallback](BENCHMARK.md#ch-028-dictionary-version-and-fallback). |
| ClickHouse | Dictionary-backed join execution | Adopted as a bounded point-lookup join path | `LookupSourceResolver` now serves direct `EXTERNAL` equality `INNER`/`LEFT` joins, and `hatDictionary.NewSQLDictionaryLookupResolver` maps typed fact keys to cached dimension values without a full dimension scan. Expected versions remain opt-in for snapshot consistency. See [CH029_DICTIONARY_BACKED_JOIN.md](CH029_DICTIONARY_BACKED_JOIN.md) and [BENCHMARK.md#ch-029-dictionary-backed-join](BENCHMARK.md#ch-029-dictionary-backed-join). |
| ClickHouse | Bitmap-backed lightweight logical deletes | Adopted as an opt-in in-memory representation optimization | Typed-table patch parts use one bitmap bit per physical row, retain the existing threshold compaction scheduler, and traverse dense live rows by bitmap word. At 100,000 rows the mask falls from 106,496 to 13,568 B/op; end-to-end delete/reinsert and dense scans did not regress in the matched measurements. See [CH012_DELETE_BITMAP.md](CH012_DELETE_BITMAP.md) and [BENCHMARK.md#ch-012-bitmap-backed-lightweight-logical-deletes](BENCHMARK.md#ch-012-bitmap-backed-lightweight-logical-deletes). |
| ClickHouse | Mutation admission throttling and maintenance windows | Adopted as an opt-in caller-owned SQL mutation gate | `SQLMutationAdmission` reserves serialized mutation slots, waits through configured daily or overnight maintenance windows, and honors context cancellation through `SQLQueryOptions.MutationAdmission`; nil remains the default with no clock or mutex work. See [CH013_MUTATION_ADMISSION.md](CH013_MUTATION_ADMISSION.md) and [BENCHMARK.md#ch-013-mutation-admission-throttling](BENCHMARK.md#ch-013-mutation-admission-throttling). |
| ClickHouse | Bitmap-backed lightweight logical deletes | Adopted as an opt-in in-memory representation optimization | Typed-table patch parts use one bitmap bit per physical row, retain the existing threshold compaction scheduler, and traverse dense live rows by bitmap word. At 100,000 rows the mask falls from 106,496 to 13,568 B/op; end-to-end delete/reinsert and dense scans did not regress in the matched measurements. See [CH012_DELETE_BITMAP.md](CH012_DELETE_BITMAP.md) and [BENCHMARK.md#ch-012-bitmap-backed-lightweight-logical-deletes](BENCHMARK.md#ch-012-bitmap-backed-lightweight-logical-deletes). |

This matrix records the ClickHouse, Materialize, and Tarantool ideas assessed
for `hatrie_cache`. An idea is adopted only when it preserves exact query or
recovery behavior and either improves a measured workload or supplies an
explicitly opt-in operational control.

| Source | Idea | Status | Evidence |
|---|---|---|---|
| Materialize | Cross-dataflow transaction visibility | Adopted as an opt-in publication barrier | `SQLDataflowVisibilityCoordinator` assigns one logical version to an explicitly prepared batch, rejects mixed-version acquires and stale tokens, and checkpoints only bounded name/version metadata. Existing SQL/materialized-view paths remain unchanged. [M-U44_TRANSACTION_VISIBILITY.md](M-U44_TRANSACTION_VISIBILITY.md) |
| Tarantool | Source-versioned SQL planner statistics | Partially adopted as explicit `ANALYZE` and typed-table stats caches | `HatTrie.AnalyzeSQLSource` retains exact row counts, null counts, distinct cardinality, numeric bounds, average value bytes, and value-frequency histograms for up to 128 source entries. With TTL disabled, `TypedTable.Stats()` and numeric `TypedTable.Histogram()` reuse exact snapshots until `Upsert` or `Delete` invalidates them; TTL-enabled tables recompute these time-sensitive snapshots instead of returning stale cached rows. Physical patch compaction preserves them. The existing what-if planner consumes current entries without decoding the source again; mutation, restore, and rollback invalidate derived state. Opt-in `SaveSQLPlannerStatistics`/`LoadSQLPlannerStatistics` HPS1 snapshots validate source SHA-256 digests; a full cost model remains deferred. [SQL_PLANNER_STATISTICS.md](SQL_PLANNER_STATISTICS.md), [C209_TYPED_TABLE_STATS.md](C209_TYPED_TABLE_STATS.md), [C210_TYPED_TABLE_HISTOGRAM.md](C210_TYPED_TABLE_HISTOGRAM.md), [BENCHMARK.md](BENCHMARK.md#tt-050-sql-planner-statistics) |
| ClickHouse | ASOF JOIN | Adopted as a constrained SQL join | Keyed right-side buckets with binary-search temporal matching; see [SQL_ASOF_JOIN.md](SQL_ASOF_JOIN.md). |
| ClickHouse | Total `GROUP BY` key limit | Adopted as an opt-in SQL resource limit | `SQLQueryOptions.MaxGroupKeys` rejects high-cardinality grouping before retaining the next key; `0` is the default, and namespace policy can tighten it through `NamespaceResourceLimits.MaxGroupKeys`. Grouped fast paths fall back when they cannot enforce the cap directly. [SQL_GROUP_KEY_LIMIT.md](SQL_GROUP_KEY_LIMIT.md) |
| ClickHouse | Sparse primary-key mark pruning | Partially adopted as ordered-index range pruning | `OrderedRangeSourceResolver` and its streaming counterpart use binary-search bounds for safe literal comparisons on indexed `ORDER BY` fields; the complete `WHERE` is still evaluated, and unavailable or unsafe shapes fall back. Physical part/mark metadata is not yet present. [SQL_ORDERED_RANGE_PRUNING.md](SQL_ORDERED_RANGE_PRUNING.md), [BENCHMARK.md](BENCHMARK.md#ordered-range-pruning) |
| ClickHouse | Partition-key pruning | Partially adopted at the SQL planner/resolver boundary | The optional `PartitionPruningSourceResolver` now receives validated literal `=`, `IN`, `<`, `<=`, `>`, and `>=` predicates, including normalized literal/field operand order. Unsafe `OR`, computed, `NULL`, sampled, or non-binary-collation shapes retain the existing partition/full-source fallback. A 64-partition fixture with one matching partition measured 17.77x lower latency and 13.91x lower allocation volume; concrete physical partition metadata remains provider-owned. [SQL_PARTITION_RANGE_PRUNING.md](SQL_PARTITION_RANGE_PRUNING.md), [BENCHMARK.md](BENCHMARK.md#sql-partition-range-pruning) |
| ClickHouse | Explicit SQL `PREWHERE` / late materialization | Adopted for stream-capable single-source reads | `PREWHERE` is evaluated before `WHERE` and projection on the narrow `StreamSourceResolver` path; specialized index, columnar, and ordered resolvers retain a combined predicate until they have an explicit two-stage contract. The measured fixture is 2.95x faster, 5.67x lower heap, and 2.29x fewer allocations. [SQL_PREWHERE.md](SQL_PREWHERE.md), [BENCHMARK.md](BENCHMARK.md#explicit-prewhere-stage) |
| Tarantool | Partition split and merge tooling | Adopted as an explicit operator planning API | `hatPartition.PlanSplit`, `PlanMerge`, and `PlanResize` validate adjacent power-of-two layouts and provide deterministic allocation-free per-key routes plus an inspectable move mapping. They do not move data or enable partitioning; automatic online migration remains deferred. [PARTITION_RESIZE.md](PARTITION_RESIZE.md) |
| Materialize | Coordinated progress frontier | Adopted | `SQLProjectionRetentionFrontier` commits journal retention only after all configured runners succeed. [PROJECTION_FRONTIERS.md](PROJECTION_FRONTIERS.md) |
| Materialize | Shared arrangements | Adopted | `TypedTableAggregateArrangements` shares exact aggregate state among identical definitions, and `TypedTableJoinArrangements` shares exact incremental equi-joins. [TYPED_TABLE_ARRANGEMENTS.md](TYPED_TABLE_ARRANGEMENTS.md), [TYPED_TABLES.md](TYPED_TABLES.md) |
| Materialize | Incremental ordered arrangements | Partially adopted as an opt-in typed-table ordinal projection cache | `TypedTableColumnarCacheOptions.SortedOrderCache` admits a bounded immutable `[]uint32` order vector after repeated compatible single-field `ORDER BY ... LIMIT` requests, reuses the existing columnar result path, and invalidates it on writes. It retains no duplicate row values and stays disabled by default; composite orders, range cursors, and arbitrary signed-update maintenance remain open. [C212_TYPED_TABLE_ORDER_CACHE.md](C212_TYPED_TABLE_ORDER_CACHE.md), [BENCHMARK.md](BENCHMARK.md#mz-038-typed-table-sorted-ordinal-projection) |
| Materialize | Arrangement memory telemetry | Adopted as an explicit opt-in diagnostics API | `TypedTableAggregateArrangements.Stats()` and `TypedTableAggregateArrangement.Stats()` report shared references, freshness, distinct values, estimated retained bytes, changelog compaction watermark, and dictionary group-order rebuild counts without materializing result rows or changing ordinary update behavior. [TYPED_TABLE_ARRANGEMENT_TELEMETRY.md](TYPED_TABLE_ARRANGEMENT_TELEMETRY.md), [BENCHMARK.md](BENCHMARK.md#mz-027-arrangement-memory-telemetry) |
| ClickHouse / Tarantool | Background-task and storage statistics | Adopted as a bounded opt-in scheduler diagnostics API | `CompactionScheduler.Stats()` reports effective concurrency, pending and running task counts, and scheduled/completed/failed callback attempts with zero allocations per read. The generic callback contract deliberately leaves bytes, age, page, cache, and WAL metrics provider-owned. [COMPACTION_SCHEDULER_STATS.md](COMPACTION_SCHEDULER_STATS.md), [BENCHMARK.md](BENCHMARK.md#tt-036-ch-027-compaction-scheduler-statistics) |
| ClickHouse | Part-merge backlog, amplification, and age metrics | Adopted as one bounded opt-in snapshot | `hatStorage.SnapshotCompactionMetrics` combines queue depth, oldest pending/running age, scheduler I/O counters, cumulative input/output bytes, and current compaction debt. It does not start background work or change scheduler defaults. [C239_COMPACTION_METRICS.md](C239_COMPACTION_METRICS.md), [BENCHMARK.md](BENCHMARK.md#c239-compaction-metrics-snapshot) |
| ClickHouse / Tarantool | Read-only backup database attachment | Adopted as an isolated SQL inspection path | `OpenBackupReadOnlyAttachment` verifies snapshot bundles, Pebble checkpoints, and incremental repositories, loads a detached trie behind `SQLSourceResolver`, and removes private staging on idempotent `Close`; live stores are never opened for writes. See [C240_READ_ONLY_BACKUP_ATTACHMENT.md](C240_READ_ONLY_BACKUP_ATTACHMENT.md) and [BENCHMARK.md](BENCHMARK.md#c240-read-only-backup-attachment). |
| ClickHouse / Tarantool | Incremental backup chunk deduplication | Adopted as the default for large incremental repository files | Pebble repository payloads larger than 1 MiB are split into verified content-addressed chunks; restore, resume restore, retention, and chain planning account for chunk objects. `BackupBundleOptions.RepositoryChunkSize` supports custom sizes and `BackupRepositoryChunkingDisabled` preserves the legacy whole-file layout. See [C241_INCREMENTAL_BACKUP_CHUNK_DEDUP.md](C241_INCREMENTAL_BACKUP_CHUNK_DEDUP.md) and [BENCHMARK.md](BENCHMARK.md#c241-incremental-backup-chunk-deduplication). |
| Materialize | Compiled reusable plan fragments | Adopted as an additive opt-in | `CompileSQLQuery` returns an immutable `CompiledSQLQuery` handle that avoids parser and prepared-cache lookup work while cloning and rebinding per execution. Existing query entry points and prepared-cache statistics remain unchanged; the handle retains one plan and is intended for repeated hot queries. |
| ClickHouse/Materialize | Freshness-keyed query result cache | Adopted | `ResultCache.Execute` takes an explicit epoch callback, invalidates stale entries, and clones cached rows/plans for caller isolation. |
| ClickHouse | Effective settings fingerprint in result-cache keys | Adopted as an additive opt-in namespace | `SQLQueryOptions.ResultCacheSettingsFingerprint` separates entries when resolver or function behavior depends on external session or tenant settings. Empty preserves the default key path; values above `MaxSQLResultCacheSettingsFingerprintBytes` bypass retention. See [C208_RESULT_CACHE_METRICS.md](C208_RESULT_CACHE_METRICS.md) and [BENCHMARK.md#ch-002-settings-aware-result-cache-keys](BENCHMARK.md#ch-002-settings-aware-result-cache-keys). |
| ClickHouse/Materialize | Persisted freshness-keyed result cache | Adopted as explicit opt-in | `hatSql.ResultCache.Persist` and `Restore` retain only typed versioned entries in a checksummed, bounded, atomic `0600` binary snapshot. `hatCache.HatTrie` exposes the same lifecycle; missing files cold-start and corrupt files never replace live entries. [CHU09_PERSISTED_SQL_RESULT_CACHE.md](CHU09_PERSISTED_SQL_RESULT_CACHE.md), [BENCHMARK.md#ch-u09-persisted-sql-result-cache](BENCHMARK.md#ch-u09-persisted-sql-result-cache) |
| ClickHouse / Materialize | Feedback-driven projection selection | Adopted as an explicit opt-in ranking aid | `SQLProjectionAdvisor` now retains bounded successful slow-query latency totals, and `CostRecommendations` orders caller-managed projection candidates by aggregate observed workload cost without changing planner defaults or creating views automatically. [CHU10_FEEDBACK_PROJECTION_SELECTION.md](CHU10_FEEDBACK_PROJECTION_SELECTION.md), [BENCHMARK.md#ch-u10-feedback-driven-projection-selection](BENCHMARK.md#ch-u10-feedback-driven-projection-selection) |
| ClickHouse | Workload-driven data-skipping-index selection | Adopted as an explicit opt-in advisory selector | `SQLIndexAdvisor.SkipIndexRecommendations` ranks supported JSON path equality candidates by bounded observed elapsed time. It retains no query text or predicate values; index creation and rebuild scheduling remain explicit, and the default query path is unchanged. [CHU11_AUTOMATIC_DATA_SKIPPING_INDEX_SELECTION.md](CHU11_AUTOMATIC_DATA_SKIPPING_INDEX_SELECTION.md), [BENCHMARK.md#ch-u11-automatic-data-skipping-index-selection](BENCHMARK.md#ch-u11-automatic-data-skipping-index-selection) |
| Materialize | Explain dataflow graph | Adopted | `BuildExplainDataflowGraph` preserves EXPLAIN steps, derives nested subplans, and emits stable pipeline/subplan edges; JSON and DOT helpers are read-only and leave the existing linear `ExplainDOT` API unchanged. |
| Materialize | Recursive differential maintenance | Adopted as an opt-in SQL data-structure API | `NewMutableIncrementalRecursiveReachability` supports exact signed edge `INSERT`/`UPDATE`/`DELETE` maintenance, recomputes only affected source nodes, and uses direct leaf deltas for isolated terminal edges. The append-only constructor remains the zero-retention default. [INCREMENTAL_RECURSIVE_REACHABILITY.md](INCREMENTAL_RECURSIVE_REACHABILITY.md) |
| Materialize / ClickHouse | User-defined retractable and mergeable aggregate states | Partially adopted as opt-in aggregate capabilities | `SQLRetractableAggregateState` and `SQLSerializableAggregateState` extend the existing merge/finalize contract without breaking legacy implementations. Registry constructors discover capabilities explicitly and return typed errors; automatic planner wiring and transactional rollback remain deferred. [MU031_RETRACTABLE_AGGREGATES.md](MU031_RETRACTABLE_AGGREGATES.md), [BENCHMARK.md](BENCHMARK.md#mu-031-retractable-aggregate-capabilities) |
| Materialize / ClickHouse | UDF purity and monotonicity classification | Partially adopted as explicit function metadata | `FunctionDefinition` now carries conservative deterministic, monotonicity, and retractable declarations; registry lookup returns defensive copies and persistence retains the metadata. No language inference or automatic planner fast path is enabled. [MU032_UDF_CAPABILITIES.md](MU032_UDF_CAPABILITIES.md), [BENCHMARK.md](BENCHMARK.md#mu-032-udf-capability-classification) |
| ClickHouse / Materialize | Generic signed differential `AVG(int64)` | Adopted as an opt-in batch operator | `GroupAverageInt64DifferentialRows` maintains exact weighted count/sum state in one pass and emits signed `float64` average transitions with atomic overflow and callback validation. It does not alter SQL defaults or planner selection. [DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md), [BENCHMARK.md](BENCHMARK.md#differential-group-average) |
| Materialize | Differential rank-window retractions | Adopted as an opt-in SQL data-structure API | `NewMutableIncrementalRankWindow` emits exact signed rank changes for row `INSERT`/`UPDATE`/`DELETE` operations and rebuilds only affected partitions; the append-only constructor remains the low-memory default. [INCREMENTAL_RANK_WINDOW.md](INCREMENTAL_RANK_WINDOW.md) |
| Materialize | Incremental `LAG`/`LEAD` windows | Adopted as an append-only SQL data-structure API | `NewIncrementalOffsetWindow` maintains bounded offset history or unresolved lead tails, emits exact lead retractions, and handles same-batch lookahead without exposing transient defaults. [INCREMENTAL_OFFSET_WINDOW.md](INCREMENTAL_OFFSET_WINDOW.md) |
| ClickHouse/Materialize | Incremental bounded frame aggregates | Adopted as an append-only SQL data-structure API | `NewIncrementalFrameWindow` maintains exact `COUNT(*)` and `SUM(int64)` values for `ROWS BETWEEN N PRECEDING AND CURRENT ROW` with bounded per-partition state, SQL NULL behavior, checked overflow, and atomic validation. [INCREMENTAL_FRAME_WINDOW.md](INCREMENTAL_FRAME_WINDOW.md) |
| Materialize | Incremental boundary windows | Adopted as an append-only SQL data-structure API | `NewIncrementalBoundaryWindow` maintains exact `FIRST_VALUE` and `LAST_VALUE` values for `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` with one retained value per partition, SQL NULL preservation, and atomic ordering/key validation. [INCREMENTAL_BOUNDARY_WINDOW.md](INCREMENTAL_BOUNDARY_WINDOW.md) |
| Materialize | Incremental `NTH_VALUE` windows | Adopted as an append-only SQL data-structure API | `NewIncrementalNthValueWindow` maintains fixed-position `NTH_VALUE` values for `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` with one retained value and a count per partition, SQL NULL preservation, and atomic ordering/key validation. [INCREMENTAL_NTH_VALUE_WINDOW.md](INCREMENTAL_NTH_VALUE_WINDOW.md) |
| ClickHouse/Materialize | Conservative SQL monotonicity inference | Partially adopted as an opt-in public analysis primitive | `AnalyzeSQLExpressionMonotonicity` proves a bounded set of ordered expression shapes, reports column dependency and possible NULL output, and returns `unknown` for unsupported or unsafe shapes. It does not alter planner defaults; schema-aware fast-path integration remains future work. [MU028_MONOTONICITY.md](MU028_MONOTONICITY.md) |
| ClickHouse/Materialize | Incremental bounded MIN/MAX frames | Adopted as an append-only SQL data-structure API | `NewIncrementalFrameWindow` maintains NULL-aware `MIN(int64)` and `MAX(int64)` values for `ROWS BETWEEN N PRECEDING AND CURRENT ROW` with bounded monotonic deques, O(1) amortized updates, and atomic ordering/key validation. [INCREMENTAL_EXTREMA_FRAME_WINDOW.md](INCREMENTAL_EXTREMA_FRAME_WINDOW.md) |
| ClickHouse/Materialize | Incremental bounded AVG frames | Adopted as an append-only SQL data-structure API | `NewIncrementalFrameWindow` maintains NULL-aware `AVG(int64)` values for `ROWS BETWEEN N PRECEDING AND CURRENT ROW` by sharing checked `SUM(int64)` and non-NULL count state, returning `float64` values with O(1) updates and atomic validation. [INCREMENTAL_AVERAGE_FRAME_WINDOW.md](INCREMENTAL_AVERAGE_FRAME_WINDOW.md) |
| ClickHouse/Materialize | Incremental bounded COUNT DISTINCT frames | Adopted as an append-only SQL data-structure API | `NewIncrementalFrameWindow` maintains exact NULL-aware `COUNT(DISTINCT int64)` values with reference-counted per-partition multiplicities, bounded frame state, atomic validation, and before/after CPU/allocation measurements. [INCREMENTAL_DISTINCT_FRAME_WINDOW.md](INCREMENTAL_DISTINCT_FRAME_WINDOW.md) |
| ClickHouse/Materialize | Validated zero-copy distinct state transitions | Adopted as an internal incremental-path optimization | `COUNT(DISTINCT int64)` completes callback and ordering validation before reusing its bounded state in place, avoiding transactional contribution/map copies; other aggregates retain copy-on-write behavior for error-producing transitions. [BENCHMARK.md](BENCHMARK.md#m065k-zero-copy-distinct-frame-state) |
| Materialize | Mutable bounded frame maintenance | Adopted as an opt-in SQL data-structure API | `NewMutableIncrementalFrameWindow` accepts exact row `INSERT`/`UPDATE`/`DELETE` mutations for all six bounded frame aggregates, rebuilds only affected partitions, emits deterministic signed retractions/insertions, and leaves the append-only constructor as the low-retention default. [INCREMENTAL_MUTABLE_FRAME_WINDOW.md](INCREMENTAL_MUTABLE_FRAME_WINDOW.md), [BENCHMARK.md](BENCHMARK.md#m065l-mutable-bounded-frame-windows) |
| ClickHouse/Materialize | Literal-independent query fingerprints | Adopted | `SQLQueryFingerprint` validates prepared or literal SQL, elides string/numeric values while retaining query shape, literal type, identifiers, operators, and parameter positions, and returns a compact SHA-256 digest. |
| ClickHouse | RowBinary-style schema-aware row transfer | Adopted as an additive opt-in | `EncodeSQLRowBinary` and `DecodeSQLRowBinary` transfer fixed-width and length-prefixed SQL row values with nullable markers and bounded malformed-input handling. `POST /api/sql` can now stream the same representation with `Accept: application/x-hatrie-rowbinary`, and `QueryRowBinaryIterator` consumes it incrementally; existing JSON/NDJSON defaults are unchanged. [SQL_ROW_BINARY.md](SQL_ROW_BINARY.md), [CH050_SQL_ROW_BINARY_STREAM.md](CH050_SQL_ROW_BINARY_STREAM.md) |
| ClickHouse | Dictionary reuse across row-transfer batches | Adopted as an additive opt-in | `NewSQLRowBinaryDictionaryEncoder` and `NewSQLRowBinaryDictionaryDecoder` retain selected string/bytes/JSON dictionaries, send additions once and ids thereafter, reject malformed stateful batches, and leave plain RowBinary unchanged. [ROW_BINARY_DICTIONARY.md](ROW_BINARY_DICTIONARY.md) |
| ClickHouse | Column statistics in transfer blocks | Adopted as an additive opt-in | `BuildSQLRowBinaryColumnStats`, `EncodeSQLRowBinaryWithStats`, and `DecodeSQLRowBinaryWithStats` carry exact counts and typed min/max metadata, recompute it during decode, and reject stale metadata without changing plain RowBinary defaults. [ROW_BINARY_STATS.md](ROW_BINARY_STATS.md) |
| ClickHouse | Kill-query control | Adopted as an opt-in operator API | `SQLQueryManager` tracks bounded privacy-safe status, rejects duplicate active IDs, and cancels through the existing SQL context checks. `Cancel` requires a bounded operator reason and returns an error that still unwraps to `context.Canceled`; ordinary SQL execution remains unregistered. [SQL_QUERY_MANAGER.md](SQL_QUERY_MANAGER.md) |
| ClickHouse | Persistent workload statistics | Adopted as an opt-in advisor snapshot | `SQLIndexAdvisor.Save` and `Load` persist bounded versioned slow-scan field and composite-prefix recommendations as JSON. Loads reject oversized, unknown, trailing, duplicate, invalid, or over-capacity state before an atomic replacement; existing query execution and advisor defaults are unchanged. [SQL_INDEX_ADVISOR.md](SQL_INDEX_ADVISOR.md) |
| ClickHouse/Tarantool | Online secondary-index build | Adopted as an opt-in background worker | `StartSQLJSONIndexRebuildWorker` consumes explicitly scheduled SQL JSON index rebuilds without starting a goroutine by default, limits each tick to one atomic unit, reports existing progress states, retries failures, and supports cooperative `Stop`/`Wait`/`Done` lifecycle control. [SQL_INDEX_REBUILD_PROGRESS.md](SQL_INDEX_REBUILD_PROGRESS.md) |
| ClickHouse | Automatic covering-index recommendation | Adopted as a read-only opt-in advisor | `SQLIndexAdvisor.CoveringRecommendations` identifies bounded simple equality projections whose selected fields could be retained by `CreateSQLJSONCoveringIndex`; columns are sorted and omit the predicate field. It never creates or activates an index, does not retain query text or literal values, and ordinary advisor recommendations/defaults remain unchanged. [SQL_INDEX_ADVISOR.md](SQL_INDEX_ADVISOR.md) |
| ClickHouse | Workload-derived primary-key prefix tuning | Adopted as a read-only opt-in advisor | `SQLIndexAdvisor.PrimaryPrefixRecommendations` records bounded ordered predicate conjunctions, puts equality fields before range fields, and returns deterministic source-local prefixes for explicit layout review. It never mutates a live layout or starts a rebuild automatically. [SQL_INDEX_ADVISOR.md](SQL_INDEX_ADVISOR.md) |
| Tarantool | Multikey indexes over array fields | Adopted as an opt-in SQL index | `CreateSQLJSONMultikeyIndex` stores one posting per distinct array element for `ARRAY_CONTAINS`, refreshes from source generations, and rechecks candidates before returning rows. Scalar, range, ordered, and non-binary-collation paths remain on their existing fallbacks. [SQL_MULTIKEY_INDEX.md](SQL_MULTIKEY_INDEX.md) |
| ClickHouse/Tarantool | Online index alteration progress, cancellation, and resume | Adopted as an opt-in queue control | `RunScheduledSQLJSONIndexRebuildsWithProgress` exposes queue-level lifecycle callbacks, checks context between atomic rebuild units, and requeues canceled or failed rebuild units for a later call. Existing index publication stays atomic, the legacy runner is unchanged, and a single rebuild is not interrupted mid-unit. [SQL_INDEX_REBUILD_PROGRESS.md](SQL_INDEX_REBUILD_PROGRESS.md) |
| Materialize/Tarantool | Explicit multi-operation transactions | Adopted as an opt-in public command API | `BATCH` with `atomic:true` serializes the command group, captures each affected key once, and rolls back in-memory values and journal entries when any subcommand fails. Ordinary `BATCH` remains the backward-compatible non-transactional pipeline, and partitioned atomic batches stay single-partition only. |
| Tarantool | Callback-scoped atomic mutation | Adopted as an opt-in embedded API | `RunAtomic` lets Go callers stage public mutation requests through `AtomicCommandBatch.Add`; the callback can abort before any write, and the complete request set is committed through the existing atomic `BATCH` executor. Requests are copied at staging time, while SQL reads and read-your-writes remain the responsibility of `BeginSQLTransaction`. |
| Materialize | Configurable transaction isolation | Adopted as an opt-in SQL transaction policy | `BeginSQLTransactionWithOptions` retains optimistic `snapshot` isolation as the zero-value default and adds `serializable`, which holds the command transaction lock until commit or rollback. `ParseSQLTransactionIsolation` accepts stable configuration names; direct typed mutations still use the existing epoch conflict guard. |
| Materialize | Savepoints and partial rollback | Already present | `SQLTransaction.Savepoint`, `RollbackTo`, and `ReleaseSavepoint` preserve earlier staged writes while discarding later writes and nested savepoints; compiler tests also cover atomic SQL savepoint programs. |
| ClickHouse | Parquet external-table import/export | Adopted | `ExternalTables` supports `ExportParquet`, `ImportParquet`, and `WriteParquet`; existing round-trip tests and `BenchmarkExternalTablesExportTransfer` cover byte and stream transfer without changing SQL defaults. |
| ClickHouse | Independently compressed transfer blocks | Adopted as an additive opt-in | `EncodeCompressedBlocks` and `DecodeCompressedBlocks` provide an `HCB1` stream with bounded blocks, raw fallback, per-block CRC32 checks, and malformed-input rejection; existing JSON/protobuf/gzip defaults remain unchanged. [COMPRESSED_BLOCKS.md](COMPRESSED_BLOCKS.md) |
| Materialize | Differential extrema | Adopted | `TypedTableAggregateDefinition` now accepts opt-in numeric `MinField` and `MaxField`. Each group retains counted distinct values and only rescans that group when its final current extreme is removed, so inserts, updates, and deletes remain exact. |
| Materialize | Differential distinct aggregate | Adopted | `TypedTableAggregateDefinition.DistinctField` retains per-group multiplicities for one opt-in typed scalar field, emitting exact `count_distinct` across inserts, updates, deletes, and replay. |
| Materialize | Differential SUM aggregate | Adopted as a reusable generic API | `GroupSumInt64DifferentialRows` maintains signed weighted `SUM` transitions for callback-defined groups, tracks multiplicity separately from the aggregate value, and rejects signed arithmetic overflow without partial output. Existing SQL defaults and typed-table aggregate paths are unchanged. [DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md) |
| Materialize | Differential reduce state sharing | Adopted as a reusable generic API | `GroupCountSumInt64DifferentialRows` updates COUNT and signed int64 SUM in one pass and emits one combined transition row. In the controlled 1,024-update benchmark it was 2.00x faster, used 0.48x heap, and used 0.58x allocations versus separate COUNT and SUM calls; existing APIs and SQL defaults remain unchanged. [DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md) |
| ClickHouse | LowCardinality typed strings | Adopted | `TypedTableColumn.DictionaryEncoded` is opt-in and retains live string values once with compact row codes; ordinary string columns retain their existing layout. |
| ClickHouse | Lookup-aware dictionary layout admission | Adopted as an additive public API | `ColumnarBatch.EncodeRepeatedStringsForLookup` combines cardinality and retained-size estimates with an explicit equality, grouping, ordering, or projection shape. Equality/grouping can admit an exact memory break-even; ordering/projection require a 10% retained-size win. `EncodeRepeatedStrings` and existing producer defaults remain unchanged. [C216_COLUMNAR_DICTIONARY_SHAPES.md](C216_COLUMNAR_DICTIONARY_SHAPES.md) |
| Tarantool | Partial equality index | Adopted | `CreateSQLJSONPartialIndex` retains postings for one lookup field only where a second field equals a fixed configured literal. The existing composite resolver selects it only for the matching equality conjunction. |
| ClickHouse/Tarantool | Reusable indexed dimension postings | Adopted | `BorrowedIndexedSourceResolver` lets an immutable ordinary HatTrie JSON equality posting list serve an eligible SQL join without cloning its candidate rows for every probe. Existing `IndexedSourceResolver` implementations remain the fallback, and hot-key plans retain the existing hash join. |
| ClickHouse | Projection for repeated ordering | Adopted | Immutable cached ordinal projections serve repeated single-column and multi-column `ORDER BY` Top-N queries, including mixed directions, after repeated reads. |
| ClickHouse | Dynamic Top-N data skipping | Adopted | Cached numeric segment min/max metadata skips only segments that cannot beat the bounded Top-N threshold; equal-boundary segments remain scanned to preserve stable ties. |
| ClickHouse | Ordered primary-key version parts | Adopted at the temporal-table boundary | `TemporalTable` keeps each key's versions time-ordered. Chronological writes append, late writes are placed after equal timestamps, and `AS OF` uses binary search while preserving copy isolation. |
| ClickHouse | Granular skipping indexes | Already present | Numeric min/max, dictionary membership, string equality Bloom, and n-gram Bloom sidecars prune only impossible segments. |
| ClickHouse | Token Bloom prefilter for word-oriented search | Adopted as a reusable conservative sidecar | `TokenBloomFilter` hashes Unicode letter/digit tokens without retaining the source text or allocating token slices. `ContainsAllTokens` and `ContainsAnyTokens` only prefilter; callers must run an exact text check after a possible match. [TOKEN_BLOOM_FILTER.md](TOKEN_BLOOM_FILTER.md) |
| ClickHouse | Narrow expression index | Adopted | `CreateSQLJSONLowerIndex` is an explicit `LOWER(field) = literal` equality index. It uses the existing generation-aware JSON snapshot lifecycle and falls back to the SQL scan for source values that are not strings. |
| Materialize | Literal semi-join index union | Adopted | Direct-field and `LOWER(direct field)` `IN (literal, ...)` predicates union existing equality postings after duplicate-literal removal. `NOT IN`, subqueries, other expressions, non-binary collations, and unavailable indexes retain the normal evaluator. |
| Tarantool | Controlled cooperative maintenance | Adopted | `ManagedRefreshScheduler` now has opt-in count and duration cycle budgets. [REFRESH_SCHEDULER.md](REFRESH_SCHEDULER.md) |
| Materialize | View freshness SLA | Adopted as an opt-in scheduler status signal | `ManagedRefreshTaskOptions.MaxStaleness` and `ManagedRefreshScheduler.Status`/`StatusAt`/`StatusesAt` report stale-before-first-success and stale-after-the-last-success threshold breaches. Existing scheduler methods keep the default disabled. [MANAGED_REFRESH_FRESHNESS.md](MANAGED_REFRESH_FRESHNESS.md) |
| Tarantool | Consistent read snapshot | Adopted | `SnapshotLocker` gives resolvers a stable query lifetime, while opt-in `TypedTableMVCCOptions` adds immutable historical typed-table snapshots without changing the default mutable path. [TYPED_TABLES.md](TYPED_TABLES.md) |
| ClickHouse/Tarantool | Immutable persistent parts / LSM storage | Adopted for persistent cache storage | `PebbleStore` uses Pebble's immutable SSTable and background-compaction path, with generation checkpoints, crash recovery, backup, and restore verification. |
| ClickHouse | Lightweight delete patch parts | Adopted for opt-in typed tables | `TypedTablePatchOptions` records delete tombstones without moving typed rows, skips them in row and columnar SQL reads, and provides threshold-triggered or explicit compaction. Updates remain on the existing path and the feature is disabled by default. [TYPED_TABLES.md](TYPED_TABLES.md) |
| ClickHouse | Dynamic JSON path skip metadata | Adopted for opt-in JSON sources | `CreateSQLJSONPathSkipIndex` stores bounded per-segment Bloom metadata for normalized nested paths. It prunes equality candidates while the SQL executor rechecks the original predicate, so false positives cannot change results. |
| Materialize | Bounded arrangement hydration | Adopted as an explicit control | `TypedTableAggregateArrangement.Hydrate` and `TypedTableJoinArrangement.Hydrate` replay retained source changes in bounded batches, with `Freshness` reports and explicit compacted-history errors. No background worker or default memory behavior changes. |
| Tarantool | Retry-safe request idempotency | Adopted as an opt-in journal control | `CommandJournal` accepts a bounded idempotency capacity, fingerprints canonical requests, returns duplicate responses without reapplying mutations, rejects key conflicts, and reconstructs the bounded state from binary or JSON journal records. Capacity `0` is the default. |
| ClickHouse/Tarantool | Byte-budgeted WAL segment retention | Adopted as an opt-in journal control | Segmented command journals accept `RetainedBytes`/`-journal-retained-bytes`, prune oldest complete unprotected segments on rotation or reopen, and keep the existing count policy and snapshot/full-sync recovery fallback. `0` is the default. |
| Materialize/Tarantool | Recovery replay progress and ETA | Adopted as an opt-in journal API | `ReplayWithProgress` exposes total discovered records, applied count, current sequence, elapsed time, best-effort ETA, and terminal errors through a concurrently pollable snapshot. Legacy replay remains unchanged. [REPLAY_PROGRESS.md](REPLAY_PROGRESS.md) |
| ClickHouse/Tarantool | Cached WAL replay metadata | Adopted as a default recovery optimization | `CommandJournal` caches the validated tail and compaction boundary while opening, checkpointing, and compacting. Ordinary `Replay`/`ReplayThrough` now apply in one journal scan; progress replay retains its counting scan. [JOURNAL_REPLAY.md](JOURNAL_REPLAY.md) |
| ClickHouse/Materialize/Tarantool | Replication queue and wire observability | Adopted as an additive monitoring surface | Existing replication results and `/metrics` expose queue depth/error counters; outgoing HTTP request wire bytes and request counts are now grouped by target and content encoding without changing the wire format or request lifecycle. [REPLICATION_METRICS.md](REPLICATION_METRICS.md) |
| ClickHouse/Materialize/Tarantool | Replication pause and resume controls | Adopted as an opt-in operational control | Async replication starts enabled and unpaused as before; operators can pause queue delivery, retain queued durable work, resume it later, inspect `Queue.Paused`, use authenticated `/api/replication` actions, and scrape a paused Prometheus gauge. Synchronous replication is unchanged. [REPLICATION_OPERATIONS.md](REPLICATION_OPERATIONS.md) |
| ClickHouse/Materialize | Transparent exact projection selection | Adopted as an opt-in SQL control | `QueryOptions.ProjectionCatalog` serves an exact materialized result only when every declared `CACHE` dependency has the same source version captured at refresh time. Unversioned sources, mismatched collations, active index hints, and non-exact queries retain the ordinary executor. [TRANSPARENT_PROJECTIONS.md](TRANSPARENT_PROJECTIONS.md) |
| Materialize | Subscription frontier and progress protocol | Adopted as an opt-in SQL control | `QuerySubscriptionDefinition` supports `AsOf`, `UpTo`, and `EmitProgress`; `NotifyChangedAt` rejects stale frontiers, evaluates historical sources through `HistoricalSourceResolver`, and closes bounded subscriptions at their upper frontier. Legacy `NotifyChanged` remains unchanged. [SUBSCRIPTION_FRONTIERS.md](SUBSCRIPTION_FRONTIERS.md) |
| ClickHouse/Materialize/Tarantool | Logical SQL publication and consumer checkpoints | Partially adopted as an opt-in publication hub | `SQLPublication` publishes contiguous schema-checked differential batches, retains bounded replay history, validates monotone checksummed consumer checkpoints, and evicts slow subscribers without blocking producers. Durable history, snapshot generation, and exactly-once connector transactions remain caller-owned. [MU027_LOGICAL_PUBLICATION.md](MU027_LOGICAL_PUBLICATION.md) |
| ClickHouse | Asynchronous insert admission and completion status | Adopted as an opt-in journal/API control | `CommandJournal.SubmitAsyncCommand` and the monitoring HTTP `X-Hatrie-Async`/`Prefer: respond-async` path admit journaled writes into the existing bounded group-commit worker; API status exposes the durable-and-applied boundary. Queue-full backpressure, request ownership copies, replay, rollback, idempotency, authentication, and replication compatibility guards remain explicit. Existing synchronous execution and defaults are unchanged. [ASYNC_COMMAND_SUBMISSION.md](ASYNC_COMMAND_SUBMISSION.md), [ASYNC_HTTP_COMMANDS.md](ASYNC_HTTP_COMMANDS.md) |
| ClickHouse/Materialize | Hypothetical index and filter-cost explanation | Adopted as a read-only SQL advisor | `ExplainSQLWhatIf` reports source rows/bytes, estimated rows/bytes skipped, index memory and mutation cost, existing-index state, and explicit recommendations. It uses optional bounded source statistics or an exact one-read fallback and never changes query results or production planning. [SQL_WHATIF.md](SQL_WHATIF.md) |
| Tarantool | Ordered `after` pagination | Adopted as an explicit SQL API | `ExecuteSQLQueryKeysetPage` and `Conn.QueryKeysetPage` seek a compatible HatTrie order index after an opaque stable value/tie cursor. Generic JSON and typed `INT64` indexes preserve duplicate and NULL order; existing offset pagination remains the default. [KEYSET_PAGINATION.md](KEYSET_PAGINATION.md) |
| Tarantool | Authenticated keyset resume tokens | Adopted as an opt-in SQL control | `SQLKeysetTokenCodec` adds bounded HMAC-SHA256 authentication and expiry to direct and partitioned keyset cursors; the legacy JSON/base64 cursor remains the default. [TR028_AUTHENTICATED_KEYSET_TOKENS.md](TR028_AUTHENTICATED_KEYSET_TOKENS.md) |
| ClickHouse/Tarantool | Runtime allocator and slab introspection | Adopted as an on-demand monitoring API | `GET /api/memory` and `hatMonitoring.ReadMemoryReport` expose live/reserved/idle/released heap, cumulative allocation counters, GC activity, runtime memory classes, and active Go memory policy. The path is read-only, no-store, authenticated with the existing monitoring middleware, and adds no per-entry or normal-operation bookkeeping. [MEMORY_REPORT.md](MEMORY_REPORT.md) |
| ClickHouse/Materialize/Tarantool | Ordered prefix filtering with borrowed candidates | Adopted as an explicit SQL index path | `LIKE 'prefix%'` on a binary-collation direct JSON string field reuses the existing ordered field index with a binary search and final predicate recheck. `BorrowedPrefixIndexedSourceResolver` optionally avoids candidate-map cloning for immutable snapshots; the cloned resolver remains the compatibility fallback. Mixed types, wildcard patterns, missing indexes, and admission denial retain the ordinary scan; no new default or per-row metadata is added. [SQL_LIKE_PREFIX_INDEX.md](SQL_LIKE_PREFIX_INDEX.md) |
| ClickHouse | Explicit asynchronous-insert acknowledgment modes | Adopted as an opt-in HTTP durability control | `wait_for_async_insert=0` preserves bounded `202` admission and `=1` waits for the existing journal submission's durable-and-applied boundary. Invalid values are rejected, request deadlines do not cancel admitted work, and the default-off async configuration plus synchronous command path remain unchanged. [CHU03_ASYNC_INSERT_ACK_MODES.md](CHU03_ASYNC_INSERT_ACK_MODES.md) |
| Tarantool | Full-text token-prefix lookup | Adopted as an opt-in SQL index path | `CONTAINS_PREFIX(field, prefix)` uses the existing JSON text postings plus a sorted token-key sidecar and binary-search prefix range. Candidates are unioned in source order and rechecked by the executor; empty or multi-token prefixes match no rows, while missing indexes retain the ordinary scan. Token-position phrase and ordered proximity search are now also available through `CONTAINS_PHRASE` and `CONTAINS_PROXIMITY`; missing indexes retain the ordinary scan. [SQL_TEXT_PREFIX_INDEX.md](SQL_TEXT_PREFIX_INDEX.md), [SQL_TEXT_PHRASE.md](SQL_TEXT_PHRASE.md) |
| ClickHouse | Runtime join Bloom filtering | Adopted as an opt-in SQL execution path | `SQLQueryOptions.RuntimeJoinBloomFilter` streams eligible direct `CACHE` inner equality joins through an exact right-side hash table preceded by a compact Bloom membership check. Unsupported shapes, available equality indexes, and non-streaming resolvers retain the established executor; the default is off because balanced joins can be slower. [SQL_RUNTIME_JOIN_FILTER.md](SQL_RUNTIME_JOIN_FILTER.md) |
| ClickHouse | `LIMIT BY` per-group top-N | Adopted as an opt-in SQL command and bounded execution path | `LIMIT n BY expression[, ...]` keeps exact per-group caps after ordering, supports composite keys, NULLs, collations, prepared expressions, global `LIMIT`/`OFFSET`, and external sort spill. Finite ordered queries use one bounded Top-N heap per group and sort only retained candidates; existing queries and defaults are unchanged. [SQL_LIMIT_BY.md](SQL_LIMIT_BY.md) |
| ClickHouse | `LIMIT WITH TIES` rank-boundary preservation | Adopted as an opt-in SQL command and materialized/spill execution path | `LIMIT n WITH TIES` requires `ORDER BY` and a finite non-negative limit, then includes adjacent rows equal on every ordering expression to the last selected row. Existing indexed/top-N paths remain unchanged; external sort spill preserves the same boundary. [LIMIT_WITH_TIES.md](LIMIT_WITH_TIES.md) |
| ClickHouse | Stable external sort run ordering | Verified in the existing SQL spill path | External `ORDER BY` records carry their input ordinal through temporary run encoding, per-run sorting, and merge passes, so equal keys retain input order across run boundaries. The focused regression forces multiple runs; no production change was needed in this round. [C228_EXTERNAL_SORT_STABILITY.md](C228_EXTERNAL_SORT_STABILITY.md) |
| ClickHouse | Incremental ordered window-frame state | Adopted automatically for a narrow running-aggregate shape | Materialized `SUM`/`AVG`/`MIN`/`MAX` windows with the default or unbounded-preceding `ROWS` frame prepare expressions once and update state per row. `RANGE`, bounded/following, exclusions, custom expressions, and non-aggregate windows retain the established evaluator. The focused 2,000-row benchmark measured 11.16x lower latency, 5.65x lower allocation volume, and 2.41x fewer allocations. [C225_INCREMENTAL_WINDOW.md](C225_INCREMENTAL_WINDOW.md) |
| ClickHouse | Grace-hash join spilling | Verified in the existing bounded SQL join path | Eligible equality joins partition both inputs into 64 hash runs, build one right-side partition chunk at a time, spill bounded output, and merge deterministically; Bloom partition skipping and cleanup are covered. Spill mode is intentionally slower but prevents an oversized in-memory build from failing or exceeding its configured memory bound. [C226_GRACE_HASH_JOIN.md](C226_GRACE_HASH_JOIN.md) |
| ClickHouse | Memory-overcommit wait queues | Adopted as an opt-in SQL execution policy | `SQLMemoryOvercommitQueue` shares bounded retained-memory reservations across concurrent materialized `GROUP BY`, `SORT`, and set operators. Queries wait for released capacity, context cancellation removes waiters, oversized requests fail immediately, and the default path remains unchanged. Queue mode costs about 1.30x CPU, 1.11x heap, and 1.47x allocations in the focused benchmark, comparable to the existing opt-in operator tracker. [C230_MEMORY_OVERCOMMIT.md](C230_MEMORY_OVERCOMMIT.md) |
| ClickHouse | Workload groups with concurrency and memory budgets | Adopted as an opt-in SQL execution policy | `SQLQueryOptions.ClusterAdmission` reserves a bounded serving or maintenance class lease per named cluster, with CPU units, memory bytes, running-query limits, queue bounds, cancellation, and observable statistics. The default remains unchanged; the focused no-contention benchmark measured 1.08x CPU, +96 B, and +1 allocation per query. [C231_SQL_WORKLOAD_GROUPS.md](C231_SQL_WORKLOAD_GROUPS.md) |
| ClickHouse | Compact hash aggregation / streamed `GROUP BY` state | Adopted automatically for a narrow exact aggregate shape | One-key direct-field `GROUP BY` queries update one compact state per group, and stream directly for `VALUES` or `CACHE` sources with `StreamSQLSource`. Richer queries and configured `MaxGroupBytes` retain the established executor, so defaults and compatibility behavior remain unchanged. The benchmark shows 1.53x lower latency, 2.18x lower allocation volume, and 1.66x fewer allocations on a 20,000-row grouped query. [SQL_HASH_AGGREGATE.md](SQL_HASH_AGGREGATE.md) |
| ClickHouse | Vectorized blocks and selection vectors | Adopted automatically for a narrow columnar grouped shape | Single-source columnar `GROUP BY` queries evaluate predicates into a reusable 1,024-row selection buffer and update compact group state without per-row source maps. Ordering, `HAVING`, joins, typed sources, and active `MaxGroupBytes` retain the established executor. The 64/1,024/20,000-row benchmark is faster and allocates less in every measured case, with up to 6.22x lower latency and 16.79x lower allocation volume. [SQL_VECTORIZED_EXECUTION.md](SQL_VECTORIZED_EXECUTION.md) |
| ClickHouse | Constant folding | Adopted for deterministic row-independent scalar expressions | `CAST`, scalar functions, `CASE`, `IN`, `BETWEEN`, null checks, comparisons, and arithmetic are evaluated during execution-local rewrite. Row-dependent, aggregate, custom, unknown, or erroring expressions retain the established evaluator. See `CONSTANT_FOLDING.md`. |
| ClickHouse/Materialize | Grouped Top-N over aggregate results | Adopted as an opt-in native SQL dataflow path | Grouped `COUNT`/`SUM` output aliases can use mixed-direction ordered `LIMIT`/`OFFSET`; compact grouped state is followed by the existing bounded Top-N heap, with stable ties and NULL semantics. Unsupported qualified source-field order, function order expressions, and `WITH TIES` remain on the ordinary path. [BENCHMARK.md](BENCHMARK.md#native-sql-dataflow-grouped-ordered-limit) |
| ClickHouse/Materialize | Aggregate `HAVING` before grouped Top-N | Adopted as an opt-in native SQL dataflow path | Selected `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` expressions in standard `HAVING` clauses are rewritten to compact grouped output values before filtering and bounded Top-N selection. Qualified, missing, unselected, custom, and windowed expressions retain the ordinary executor. [BENCHMARK.md](BENCHMARK.md#native-sql-dataflow-grouped-having) |
| ClickHouse/Materialize | Typed string arrangements for grouped dataflow | Adopted as an opt-in native SQL dataflow path | Direct string `GROUP BY` keys use a lazy string-to-group index beside the integer/`NULL` indexes, so repeated string keys avoid formatted composite keys while unsupported runtime values remain fail-closed. Empty strings, `NULL`, binary ordering, selected aggregate `HAVING`, and pagination preserve ordinary SQL results. [BENCHMARK.md](BENCHMARK.md#native-sql-dataflow-string-group) |
| ClickHouse/Materialize | Typed string arrangements for distinct dataflow | Adopted as an opt-in native SQL dataflow path | Direct string `DISTINCT` keys use a lazy string membership index beside the integer/`NULL` indexes, preserving first-seen order, scalar filtering, empty strings, `NULL`, and fail-closed unsupported values. [BENCHMARK.md](BENCHMARK.md#native-sql-dataflow-string-distinct) |
| ClickHouse/Materialize | Fixed typed composite arrangements for distinct dataflow | Adopted as an opt-in native SQL dataflow path | Two direct `DISTINCT` fields use a fixed comparable key with typed integer, string, and `NULL` components, avoiding formatted composite keys and dynamic per-row key slices while preserving first-seen order and fail-closed unsupported values. [BENCHMARK.md](BENCHMARK.md#native-sql-dataflow-composite-distinct) |
| ClickHouse | Two-level local columnar aggregation | Adopted as an explicit `Workers >= 2` SQL control | High-cardinality columnar grouped queries with at least two `COUNT`/`MIN`/`MAX` projections build local states over contiguous ranges and merge them in range order. The default sequential path is unchanged; small inputs, custom functions, `SUM`/`AVG`, richer queries, and configured group-memory budgets fall back. On 32K rows and 257 groups, the measured two-worker path was 1.32x faster than the pre-feature control, with 1.41x higher allocation volume, so it remains opt-in. [SQL_TWO_LEVEL_AGGREGATION.md](SQL_TWO_LEVEL_AGGREGATION.md) |
| ClickHouse | `argMax`/`argMin` aggregates | Adopted automatically | `ARGMAX(payload, ordering_value)` and `ARGMIN(payload, ordering_value)` support ordinary, grouped, filtered, and window aggregates. Eligible global field/literal scans use bounded constant state and simple literal predicates are applied during source traversal; complex shapes retain the general evaluator. NULL operands are skipped and ties preserve first-seen order. [SQL_ARG_EXTREME.md](SQL_ARG_EXTREME.md) |
| ClickHouse | Aggregate `If` combinators | Adopted as syntax-compatible conditional aggregates | `COUNT_IF`/`COUNTIF`, `SUM_IF`/`SUMIF`, `AVG_IF`/`AVGIF`, `MIN_IF`/`MINIF`, `MAX_IF`/`MAXIF`, and conditional arg-extremes normalize to the existing aggregate filter state. Global basic aggregates retain the constant-state stream path; grouped/HAVING queries preserve normal semantics, and unsupported conditional window forms fail explicitly. [SQL_AGGREGATE_IF.md](SQL_AGGREGATE_IF.md) |
| ClickHouse / Materialize | SQL aggregate `State` and `Merge` combinators | Adopted as opt-in built-in SQL functions | `COUNT_STATE`/`COUNT_MERGE`, `SUM_STATE`/`SUM_MERGE`, `AVG_STATE`/`AVG_MERGE`, `MIN_STATE`/`MIN_MERGE`, and `MAX_STATE`/`MAX_MERGE` exchange strict compact `HAST` binary states. Existing aggregate syntax, storage formats, and defaults remain unchanged; `OrNull` remains open. See [AGGREGATE_COMBINATORS.md](AGGREGATE_COMBINATORS.md) and [BENCHMARK.md](BENCHMARK.md#ch-036-sql-aggregate-state-and-merge). |
| ClickHouse / Materialize | Arg-extreme aggregate state and merge | Adopted as opt-in built-in SQL functions | `ARGMAX_STATE`/`ARGMIN_STATE` encode the selected and ordering values in strict `HAEX` binary states; matching merge functions preserve NULL skipping, stored collation, and first-winner ties. Exact scalar types are retained across transfer, while ordinary arg-extreme queries remain unchanged. See [AGGREGATE_COMBINATORS.md](AGGREGATE_COMBINATORS.md) and [BENCHMARK.md](BENCHMARK.md#ch-037-argmin-argmax-aggregate-state). |
| ClickHouse | Streaming approximate aggregate state | Adopted as a narrow global execution path | `APPROX_COUNT_DISTINCT` and `APPROX_PERCENTILE` now feed the existing HyperLogLog and quantile sketch directly from streaming source rows. Direct field/literal arguments avoid per-row execution maps; grouped, top-k, invalid, and unsupported shapes retain the established path. [SQL_APPROXIMATE_STREAM.md](SQL_APPROXIMATE_STREAM.md) |
| ClickHouse | Grouping identifiers for grouping sets | Adopted in the existing grouping-set rewrite | `GROUPING(expr)` is folded to `0` or `1` for each `GROUPING SETS`, `ROLLUP`, or `CUBE` branch, while ordinary `GROUP BY` returns `0`. Invalid grouping arguments and pre-aggregation use fail explicitly. [SQL_GROUPING_IDENTIFIERS.md](SQL_GROUPING_IDENTIFIERS.md) |
| ClickHouse | Time-series `WITH FILL` and interpolation policies | Adopted as bounded opt-in SQL behavior | Ordered timestamp results can fill `[FROM, TO)` buckets and optionally apply `PREVIOUS`, `NEXT`, or numeric `LINEAR` values to generated rows. Existing rows and unconfigured fill queries retain their established behavior; result budgets still bound expansion. [C218_WITH_FILL_INTERPOLATION.md](C218_WITH_FILL_INTERPOLATION.md) |

| Tarantool | Disk-space reserve admission | Adopted as an opt-in storage guard | The CLI flag `-db-storage-disk-reserve-bytes` and `ConfigurePersistentStoreDiskReserveBytes` keep a minimum physical free-space reserve before persistent writes. The default is `0`; logical `db-storage-max-bytes` remains separate. Unsupported free-space probes fail closed. [PERSISTENT_STORAGE_DISK_RESERVE.md](PERSISTENT_STORAGE_DISK_RESERVE.md) |
| ClickHouse/Tarantool | Run-level Bloom filters | Adopted as an opt-in persistent-read control | LevelDB and Pebble can write native 10-bits/key Bloom filters for new runs, and the replication outbox uses the same setting. `-db-storage-bloom-filter-bits-per-key` and `ConfigurePersistentStoreBloomFilterBitsPerKey` default to `0` because the measured warm workload showed only a 1.02x LevelDB miss improvement, slower hits, no clear Pebble win, and 11.7%-14.4% more compacted bytes. [PERSISTENT_STORE_BLOOM_FILTER.md](PERSISTENT_STORE_BLOOM_FILTER.md), [BENCHMARK.md](BENCHMARK.md#tt-017-opt-in-persistent-store-run-level-bloom-filters) |
| Materialize/Tarantool | Schema migration dry run | Adopted as an importable schema control-plane API | `hatSchema.Preview` validates and applies `Up` changes to an independent schema clone without publication; `hatSchema.Apply` uses the same validation path. It is metadata-only and does not validate existing row contents. [SCHEMA_MIGRATION_DRY_RUN.md](SCHEMA_MIGRATION_DRY_RUN.md) |
| Materialize | Dependency invalidation graph | Adopted automatically for materialized-view refresh planning | `MaterializedViews` keeps a compact reverse source-to-view index, so `RefreshChanged` avoids scanning unrelated views, deduplicates multi-source overlaps, and preserves deterministic ordering and atomic failure behavior. [MATERIALIZED_VIEW_DEPENDENCY_GRAPH.md](MATERIALIZED_VIEW_DEPENDENCY_GRAPH.md), [BENCHMARK.md](BENCHMARK.md#mz-042-materialized-view-dependency-invalidation) |
| ClickHouse | Persistent query log | Adopted as an opt-in operator history | `OpenSQLQueryLog` appends terminal `SQLQueryManager` status as strict NDJSON with `0600` permissions, sanitized fields, explicit `Sync`, restart-readable history, and non-fatal `QueryLogError` reporting. The default manager remains in-memory-only. [PERSISTENT_QUERY_LOG.md](PERSISTENT_QUERY_LOG.md), [BENCHMARK.md](BENCHMARK.md#ch-031-persistent-sql-query-log) |

| ClickHouse | Direct columnar append ingestion | Partially adopted as `hatSql.TypedTable.AppendColumnar`; complete scalar batches are validated before mutation and appended column-wise with plain and packed layouts supported. SQL `INSERT` routing remains caller-owned. | [CHU22_DIRECT_COLUMNAR_APPEND.md](CHU22_DIRECT_COLUMNAR_APPEND.md), [BENCHMARK.md#ch-u22-direct-columnar-append](BENCHMARK.md#ch-u22-direct-columnar-append) |
| ClickHouse | Async-insert queue status and explicit flush | Adopted as the bounded importable `AsyncInsertQueueRegistry` with payload-free status snapshots and authenticated targeted/all-queue flush endpoints. It is disabled unless explicitly configured. | [CHU23_ASYNC_INSERT_QUEUE.md](CHU23_ASYNC_INSERT_QUEUE.md), [BENCHMARK.md#ch-u23-async-insert-queue-status-and-flush](BENCHMARK.md#ch-u23-async-insert-queue-status-and-flush) |

## Measured Results

| Feature | Result |
|---|---|
| Shared typed aggregate arrangement, two consumers over 10,000 changes | 2.02x faster, 1.98x less heap, and 1.99x fewer allocations than two independent aggregates. |
| Shared typed equi-join arrangement, one updated row with 10,000 rows per side and 64 string join keys | About 16.4 us, 1.0 KB, and 5 allocations, versus a 1.53 s, 634 MB, 3,175,006-allocation full rebuild: about 93,000x faster, 634,000x less allocated heap, and 635,000x fewer allocations. Factorized structural pairs retain source keys without cloning row values or serializing pair identities; same-kind string keys avoid a redundant prefix. It is explicit because retained state grows with matching-pair cardinality. |
| Typed equi-join coalesced same-key changes, two updates and 1,024 matches | About 2.56 us, 344 B, and 6 allocations, versus separate updates at 197 us, 192 KB, and 32 allocations: about 77x faster, 557x less allocated heap, and 5.3x fewer allocations. It preserves ordered validation and checkpoint-prefix behavior. |
| Incremental typed-table MIN/MAX, one non-extreme update in a 10,000-row group | Median about 1.61 us, 1.37 KB, and 14 allocations, versus a full min/max rescan at 2.98 ms, 4.64 MB, and 49,745 allocations: about 1,850x faster, 3,390x less allocated heap, and 3,550x fewer allocations. The per-group value multiset is retained only when `MinField` or `MaxField` is configured; removing a final current extreme rescans that group's distinct values. |
| Incremental typed-table COUNT DISTINCT, one non-final-value update in a 10,000-row group with 1,000 values | Median about 1.48 us, 1.35 KB, and 14 allocations, versus a full distinct-set rescan at 3.28 ms, 4.66 MB, and 47,446 allocations: about 2,200x faster, 3,400x less allocated heap, and 3,390x fewer allocations. The per-group value multiplicity map is retained only when `DistinctField` is configured; NULL is excluded. |
| Opt-in typed-table dictionary strings, 10,000 rows and 8 values | Median about 183 us and 188 KB, versus plain string headers at 182 us and 713 KB: effectively equal construction time with about 3.8x less retained allocation. It adds eight setup allocations for dictionary bookkeeping and is disabled by default. |
| Packed dictionary values, 4,096 rows and 16 values | Dictionary-value payload fell from 338 to 166 bytes (2.04x less); total batch layout fell from 16,722 to 16,550 bytes (1.01x less) because the unchanged row-code array dominates. Five-run lookup medians were 180,155 ns/op versus 177,545 ns/op, with identical 65,536 B/op and 4,096 allocations/op. `PackDictionaryValues` is explicit opt-in. |
| Packed arrangement dictionary codes, 4,096 rows and 8 values | Retained row-code storage fell from 16,384 bytes of `uint32` codes to 4,096 byte-packed codes (4.00x less). Five-run dictionary scan query medians were 26,070 ns/op versus 26,890 ns/op (1.03x slower), with identical 5,336 B/op and 39 allocations/op. `PackDictionaryCodes` is explicit opt-in; typed-table and vertical-merge defaults retain legacy `Codes` for compatibility and predictable CPU cost. |
| Packed nullable arrangement columns, 4,096 rows and 75% NULLs | Retained column payload fell from 65,536 bytes of interface slots to 18,948 bytes of validity, rank, and dense-value storage (3.46x less). Five-run full column scans were 55,754 ns/op versus 65,565 ns/op (1.18x slower), with identical zero query allocations. `PackNullableColumns` is explicit opt-in and skips layouts whose estimated storage would not shrink. |
| Bit-packed boolean arrangement columns, 4,096 all-valid rows | Retained value storage fell from 65,536 bytes of interface slots to 512 bitmap bytes (128x less). Five-run full column scans were 72,228 ns/op versus 82,737 ns/op (1.15x slower), with identical zero query allocations. `PackBooleanColumns` is explicit opt-in; nullable booleans use a second validity bitmap. |
| Opt-in partial JSON equality index, 10,000 rows with `active = true` at 10% selectivity | Median refresh about 217 us, 25.9 KB, and 1,013 allocations, versus a general composite index at 1.10 ms, 568 KB, and 20,031 allocations: about 5.1x faster, 22x less allocated heap, and 19.8x fewer allocations. It is used only when the query includes the configured fixed equality. |
| Borrowed equality postings, 100 fact probes into a 10,000-row indexed dimension | Median about 546 us, 531 KB, and 2,589 allocations, versus copied postings at 588 us, 565 KB, and 2,889 allocations: about 1.08x faster, 1.06x less allocated heap, and 1.12x fewer allocations. A hot 10,000-probe one-row dimension was rejected: forcing indexed probes was about 1.28x slower, 1.14x more heap, and 1.20x more allocations than the retained hash join. |
| Runtime join Bloom filter, 100,000 left rows | With 512 right rows, the opt-in path was 2.90x faster, used 14.01x less heap, and performed 2.85x fewer allocations. With 1,024 matching rows on each side it was 1.10x slower, used 1.09x less heap, and performed 1.07x more allocations; with one hot right key it was 1.36x faster and used 1.56x less heap, at 1.10x more allocations. The default remains unchanged and off. |
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
| Retry-safe command idempotency, duplicate `INC` retry | Median 4.371 us with capacity `0` versus 1.100 us with capacity `16`: 3.97x lower duplicate-hit latency with the same 256 B and two allocations in this fixture. Eight-command keyed group commit was 0.775 ms versus 0.760 ms, effectively CPU-neutral amid filesystem sync jitter, while keyed state cost 1.52x heap and 1.41x allocations. A short keyed journal record was 89 B binary versus 42 B unkeyed, and 202 B JSON versus 93 B unkeyed. |
| Transparent exact projection selection, four-row source | Median 2.25x faster, 2.20x less allocated heap, and 2.65x fewer allocations than direct query execution. The result remains cloned for caller isolation, and source-version checks keep stale snapshots off the hit path. |

| Opt-in typed-table MVCC snapshots, 1,000-row writes and current-snapshot reads | Median repeated writes measured 1.12x slower with 18% more heap and two additional allocations; current-snapshot row materialization was 1.10x slower with 0.1% more heap and four additional allocations. Historical reads are the benefit, so MVCC remains disabled by default. |
| Opt-in typed-table lightweight delete patches, 10,000 rows | Delete followed by reinsert was 1.16x faster with 5.9% less heap and the same four allocations; 50%-deleted `Rows` was 1.03x faster with the same allocation profile. Explicit compaction cost 35.4 us for 1,000 rows and 0.52 ms for 10,000 rows. Tombstoned backing capacity remains until the normal storage lifecycle releases it, so this optimizes row movement rather than immediate heap reclamation. |
| Opt-in JSON path skip metadata, 20,000 rows and 100 nested matches | Warmed nested equality execution was about 4.77x faster, used about 2.91x less allocated heap, and made about 6.91x fewer allocations than the full JSON scan. The index retains 512 bits per 256-row segment in this fixture and remains disabled unless configured. |
| SQL what-if advisor, 10,000-row exact fallback and 100,000-row statistics path | Exact fallback: about 5.9 ms, 2.7 MB, and 80,017 allocations per report. Statistics path: about 2.4 us, 3.5 KB, and 19 allocations. The advisor is read-only; statistics are the recommended production path. |
| SQL keyset pagination, 100,000-row HatTrie source at page 900 | Median 57.86 us, 103.83 KB, and 741 allocations versus offset pagination at 48.60 ms, 86.43 MB, and 400,062 allocations: about 840x lower latency, 832x lower heap, and 540x fewer allocations. It is explicit and index-shape limited; offset remains the default. See [KEYSET_PAGINATION.md](KEYSET_PAGINATION.md). |
| JSON `LIKE 'prefix%'`, 10,000-row source with 5% matches | Median 137.6 us, 89,673 B, and 1,529 allocations through the borrowed ordered prefix index versus 15.465 ms, 9,922,114 B, and 160,048 allocations on the scan baseline: 112.4x faster, 110.6x less allocated heap, and 104.7x fewer allocations. Against the cloned indexed path, borrowing is 1.92x faster, 2.92x lower heap, and 1.65x fewer allocations. The index is explicit and mixed or complex patterns fall back. See [BENCHMARK.md](BENCHMARK.md#json-like-prefix-index). |
| ClickHouse `LIMIT BY`, 10,000 rows and 100 groups with `ORDER BY score DESC LIMIT 2 BY region` | Median 7.097 ms and 8,571,711 B with the bounded per-group Top-N path, versus 24.707 ms and 9,226,462 B for sorting and returning every row: 3.48x faster and 1.08x lower heap. It used 40,444 allocations versus 50,027: 1.24x fewer. The result retained 200 rows; source/projected row materialization remains the compatibility baseline. Raw samples and command are in [BENCHMARK.md](BENCHMARK.md#sql-limit-by). |
| ClickHouse vectorized grouped execution, 64/1,024/20,000 columnar rows | Median 1.12x/2.18x/6.22x faster, 1.19x/2.72x/16.79x lower allocation volume, and 1.12x/1.78x/2.85x fewer allocations than the row executor. The 1,024-row selection buffer is bounded per query; no storage or wire format changes. Raw samples and fallback rules are in [BENCHMARK.md](BENCHMARK.md#columnar-vectorized-grouped-aggregates) and [SQL_VECTORIZED_EXECUTION.md](SQL_VECTORIZED_EXECUTION.md). |
| ClickHouse `LIMIT WITH TIES`, 100 unique ordered rows | Five-run median 70,233 ns, 107,047 B, and 628 allocations for `LIMIT 10 WITH TIES`, versus 75,253 ns, 119,720 B, and 930 allocations for ordinary `LIMIT 10` in the same feature build: 1.07x lower latency, 1.12x lower allocated heap, and 1.48x fewer allocations in this workload. The ordinary path remained within 1.7% of the `origin/master` baseline median (74,004 ns); this is not a universal speed claim because the feature is opt-in and uses a different materialization path. Raw samples and command are in [LIMIT_WITH_TIES.md](LIMIT_WITH_TIES.md). |

| ClickHouse | Common-subexpression elimination | Adopted for exact pure boolean duplicates | The execution rewrite collapses structurally identical deterministic subexpressions in `A AND A` and `A OR A` predicates. It excludes custom functions, subqueries, windows, filtered aggregates, and query-dependent expressions, so ordinary queries have no runtime cache or storage-format cost. [BENCHMARK.md](BENCHMARK.md#sql-common-subexpression-elimination) |

| Materialize | Refreshable materialized views with explicit refresh policy | Already present / adopted | `MaterializedViews` publishes immutable snapshots with dependency-scoped atomic refresh, `ManagedRefreshScheduler.AddMaterializedView` provides fixed intervals, and the disabled-by-default `IncrementalProjectionRunner` provides ordered journal coalescing and durable checkpoints. See [INCREMENTAL_PROJECTIONS.md](INCREMENTAL_PROJECTIONS.md) and the benchmark in [BENCHMARK.md](BENCHMARK.md). |
| ClickHouse / Tarantool | Durable mutation queue with observable progress | Already present / adopted | `CommandJournal` and the LevelDB-backed `ReplicationOutboxStore` provide durable FIFO recovery, bounded restart restore, dead letters, and compatibility codecs; `ReplayWithProgress` exposes concurrent replay progress and ETA. See [REPLAY_PROGRESS.md](REPLAY_PROGRESS.md) and the replication/outbox benchmark in [BENCHMARK.md](BENCHMARK.md). |
| ClickHouse / Materialize | OpenTelemetry spans for query phases | Implemented, opt-in | `QueryTraceRecorder.OpenTelemetrySpans` emits SDK-neutral query root and operator child spans with OTLP-compatible IDs, statuses, counters, independent attributes, and no SQL/error/value retention. See [QUERY_TRACING.md](QUERY_TRACING.md). |
| ClickHouse | Fixed-width numeric arrangement vectors | Adopted as an additive opt-in | `ColumnarBatch.PackNumericColumns` stores homogeneous `int64`/`float64` values in fixed-width words with an optional NULL bitmap; `Value`, SQL predicates, and vertical merges preserve logical values, while legacy `Columns` remains the default. The controlled 4,096-row benchmark cuts layout bytes 2.00x with 0 allocations and measures a 1.16x lookup CPU cost. |

| Tarantool | Declarative row-level SQL triggers | Adopted as an explicit API | `ParseSQLTriggerDefinition` validates `AFTER ... FOR EACH ROW` DDL and `SQLTriggerRegistry.RegisterSQLTrigger` binds it to the existing caller-owned transactional trigger coordinator; unsupported `BEFORE` timing and automatic DML wiring remain rejected. See [SQL_TRIGGERS.md](SQL_TRIGGERS.md). |
| Tarantool | Visibility-timeout queue leases | Adopted as importable generic data structures | `hatDataStructure.VisibilityQueue` provides bounded or unbounded leases, acknowledgements, delayed negative acknowledgements, retry attempt counts, timeout recovery, and O(log n) deadline removal without steady-state allocations. `PriorityVisibilityQueue` adds lower-number-first priority ordering, delayed readiness, epoch-fenced claim tokens, CRC-protected binary checkpoints, and atomic restart recovery; checkpoint frequency remains caller-owned. See [VISIBILITY_QUEUE.md](VISIBILITY_QUEUE.md), [PRIORITY_VISIBILITY_QUEUE.md](PRIORITY_VISIBILITY_QUEUE.md), and [BENCHMARK.md](BENCHMARK.md#priority-visibility-queue). |
| Tarantool | Net.box-style pooled peer connections | Adopted as an importable protocol-neutral pool | `hatReplication.ConnectionPool` bounds open connections, reuses idle clients, honors context cancellation, supports explicit discard after transport failure, wakes waiters on close, and reports pool stats. Dialing, authentication, TLS, reconnect, and backoff remain caller-owned. See [CONNECTION_POOL.md](CONNECTION_POOL.md) and [BENCHMARK.md](BENCHMARK.md#connection-pool-reuse). |

| Materialize | Frontier-bound immutable SQL snapshots | Adopted as an explicit provider contract | `BeginSQLFrontierSnapshot` waits for every indexed source partition and requires `BeginSQLSnapshotAt` to bind the physical immutable view to the exact frontier; legacy providers are rejected rather than silently weakened. The default SQL path remains unchanged. [SQL_FRONTIER_SNAPSHOTS.md](SQL_FRONTIER_SNAPSHOTS.md) |
| Materialize | Executable reusable dataflow fragments | Adopted as an additive callback-backed API | `CompileSQLDataflow` and `CompiledSQLQuery.CompileDataflow` validate and snapshot lowered plans, expose upstream outputs without per-fragment input-list allocation, and preserve cancellation and error boundaries. Operator semantics remain caller-owned; existing SQL execution and defaults are unchanged. [SQL_DATAFLOW_EXECUTOR.md](SQL_DATAFLOW_EXECUTOR.md) |
| Materialize | Built-in executable dataflow operators | Adopted as an opt-in native scalar, global-aggregate, grouped, and distinct batch path | `CompileNativeDataflow` fuses built-in scalar filtering/projection, aggregate-only `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, one-direct-integer-key grouped aggregate execution, and one-direct-integer-key `DISTINCT` execution for already-resolved single-source `CACHE`/`KEYS` batches. Unsupported joins, mixed projections, multiple grouping expressions, `HAVING`, windows, ordering, and other shapes fail with `ErrSQLNativeDataflowUnsupported`; ordinary SQL execution remains the fallback. The paired 4,096-row scalar benchmark is 1.86x faster with 3.69x fewer bytes and 3.00x fewer allocations; global aggregates are 1.71x faster with 2.17x fewer bytes and 1.34x fewer allocations; grouped aggregates are 2.08x faster with 2.18x fewer bytes and 2.05x fewer allocations; distinct execution is 4.91x faster with 11.13x fewer bytes and 41.34x fewer allocations. [SQL_DATAFLOW_EXECUTOR.md](SQL_DATAFLOW_EXECUTOR.md#built-in-native-batch-path) |
| ClickHouse / Materialize | Finite scalar `LIMIT`/`OFFSET` early termination | Adopted as an opt-in extension of native dataflow execution | `CompileNativeDataflow` accepts finite windows for plain single-source scalar filter/project batches, applies offset after `WHERE`, and stops before evaluating later source rows once the page is full. Grouped, aggregate, and `DISTINCT` windows fail closed; ordinary SQL execution and defaults are unchanged. On 4,096 rows with `LIMIT 32 OFFSET 512`, the paired median is 35.13x faster, with 309.98x fewer bytes and 245.19x fewer allocations. [SQL_DATAFLOW_EXECUTOR.md](SQL_DATAFLOW_EXECUTOR.md#native-limit-measurement) |
| ClickHouse / Materialize | Bounded ordered Top-N pages | Adopted as an opt-in extension of native dataflow execution | `CompileNativeDataflow` reuses the existing `sqlTopNStreamHeap` for one direct source-field `ORDER BY` with finite `LIMIT`/`OFFSET`, retains only `LIMIT+OFFSET` candidates, preserves comparator and tie/NULL ordering, and projects only the final page. Projected aliases and unsupported stateful shapes remain on ordinary execution. On 4,096 rows with `LIMIT 32 OFFSET 512`, the paired median is 4.30x faster, with 35.66x fewer bytes and 32.35x fewer allocations. [SQL_DATAFLOW_EXECUTOR.md](SQL_DATAFLOW_EXECUTOR.md#native-ordered-limit-measurement) |
| ClickHouse / Materialize | Composite ordered Top-N pages | Adopted as an opt-in extension of native dataflow execution | `CompileNativeDataflow` extends the bounded `sqlTopNStreamHeap` path to multiple direct source-field order keys with per-key direction, stable ties, and existing NULL semantics. Projected aliases and non-field order expressions remain fail-closed. On 4,096 rows with `LIMIT 32 OFFSET 512`, the paired median is 4.34x faster, with 16.16x fewer bytes and 4.35x fewer allocations. [SQL_DATAFLOW_EXECUTOR.md](SQL_DATAFLOW_EXECUTOR.md#native-composite-ordered-limit-measurement) |
| ClickHouse / Materialize | Fixed typed composite grouping keys | Adopted as an opt-in extension of native dataflow execution | `CompileNativeDataflow` groups by two direct source fields with a fixed comparable key whose components support integers, strings, and NULL, then reuses the existing aggregate state. First-seen group order is retained; ordered, HAVING, windowed, and unsupported key shapes remain on ordinary SQL execution. On 20,000 rows, the paired median is 3.69x faster with 3.92x fewer bytes and 15.48x fewer allocations. [SQL_DATAFLOW_EXECUTOR.md](SQL_DATAFLOW_EXECUTOR.md#native-composite-group-measurement) |

| Tarantool / ClickHouse | Reverse ordered index iterators | Adopted as an additive zero-copy traversal API | `OrderedIndex.Last`, `SeekBefore`, and `SeekBeforeOrEqual` expose descending live iteration; `LastSnapshotCursor` and reverse cursor seeks preserve a stable view through existing copy-on-write snapshots. No default path, storage format, wire format, or SQL planner behavior changes. The 1,024-entry benchmark is 1.62x faster and 24,576 B/op lower than an allocating materialize-and-reverse baseline, while an already-reused slice remains faster. See [TR029_REVERSE_ITERATORS.md](TR029_REVERSE_ITERATORS.md) and [BENCHMARK.md](BENCHMARK.md#tr-029-reverse-ordered-index-iterators). |

## CH-057: Journal-Backed SQL Mutation Idempotency

ClickHouse-style insert deduplication is adopted as an explicit SQL mutation
API. `ExecuteSQLMutationIdempotent` stores a bounded caller token with the
generated command fingerprint in the existing command journal, suppresses
duplicate direct and atomic `INSERT ... SELECT` writes, rejects conflicting
reuse, and survives journal replay. The ordinary SQL mutation API remains
unchanged; `RETURNING`, `ON CONFLICT`, `MERGE`, and automatic triggers remain
unsupported because their semantics are not represented by the public journal
record. See [CH057_SQL_MUTATION_IDEMPOTENCY.md](CH057_SQL_MUTATION_IDEMPOTENCY.md).

## CH-U08: Automatic SQL Result-Cache Wiring

Direct `HatTrie` materialized SQL entry points can opt into a bounded per-trie
result cache with `ConfigureSQLResultCache`. The default is off. The trie
mutation epoch provides conservative invalidation, and explicit caller caches
remain authoritative. See [CHU08_AUTOMATIC_SQL_RESULT_CACHE.md](CHU08_AUTOMATIC_SQL_RESULT_CACHE.md).

## CH-U09: Persisted SQL Result Cache

The explicit result cache can persist versioned SQL entries for warm starts.
`Persist` writes a bounded checksummed binary snapshot and `Restore` validates
the full file before replacing typed entries. Source versions and exact query
keys remain authoritative, so schema/settings changes miss normally. The
feature is opt-in and has no background filesystem writer. See
[CHU09_PERSISTED_SQL_RESULT_CACHE.md](CHU09_PERSISTED_SQL_RESULT_CACHE.md).

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

### Ordered range pruning / primary-key marks

Status: partially adopted. Existing ordered generic and typed JSON indexes now
expose an optional range resolver. The planner recognizes one direct literal
comparison on the same field as a single-field `ORDER BY` and uses binary-search
bounds before evaluating the complete predicate. The stream path also feeds the
bounded Top-N operator from that range, so the materialized API and `QueryRows`
both avoid scanning the lower ordered prefix. OR and other unsafe shapes retain
the old resolver path. The physical ClickHouse-style part/mark index remains a
future storage-layer improvement. See
[SQL_ORDERED_RANGE_PRUNING.md](SQL_ORDERED_RANGE_PRUNING.md) and the raw
[benchmark results](BENCHMARK.md#ordered-range-pruning).

| ClickHouse | Typed compact keys for grouped arrangement state | Implemented | `TypedTableAggregate` hashes typed group values without allocating a formatted key on every mutation, uses exact collision buckets, and retains one legacy key per live group for deterministic row ordering. See [BENCHMARK.md](BENCHMARK.md#typed-aggregate-arrangement-hash-keys). |
| Materialize | Differential checkpoint export/import | Implemented as an explicit HDF1 API | `EncodeDifferentialCheckpoint` and `DecodeDifferentialCheckpoint` provide bounded deterministic typed checkpoints with CRC32C validation; `DifferentialDataflow.ImportCheckpoint` applies rows and advances the frontier atomically after the existing batch-atomic sink succeeds. HDF1 is explicit and JSON compatibility remains caller-selected. [M-U46_DIFFERENTIAL_EXPORT.md](M-U46_DIFFERENTIAL_EXPORT.md) |
| Materialize | Progress-only subscription frontier frames | Implemented as an explicit QPF1 API | `EncodeQuerySubscriptionProgressFrame` and `DecodeQuerySubscriptionProgressFrame` transfer subscription id, revision, frontier, and completion without row data. The versioned frame is bounded and CRC32C-checked; existing JSON and data-bearing subscription paths remain unchanged. [M-U47_PROGRESS_FRAMES.md](M-U47_PROGRESS_FRAMES.md) |

## M051c: Immutable Compiled SQL Template Reuse

Inspired by prepared-plan reuse in ClickHouse-style query execution, static
compiled SQL handles now reuse their immutable rewritten template. Parameterized
queries and calls with dynamic collation, optimizer, or index-hint options keep
the clone path. See [COMPILED_TEMPLATE_REUSE.md](COMPILED_TEMPLATE_REUSE.md).

## M065m: Peer-Aware Incremental Numeric RANGE Windows

Materialize-style differential maintenance now supports opt-in append-only
numeric `RANGE` frames for `COUNT(*)` and `SUM(int64)`. Equal-order peers emit
exact retractions and replacements; the legacy ROWS path remains unchanged.
See [INCREMENTAL_RANGE_WINDOW.md](INCREMENTAL_RANGE_WINDOW.md).
## M065n: Peer-Aware Incremental `RANGE` Extrema

Adopted a narrow incremental window-function slice inspired by streaming SQL
engines: numeric `RANGE` `MIN(int64)` and `MAX(int64)` now maintain a
monotonic deque over the active frame. Peer rows are updated together, NULL
values are ignored, and empty frames return NULL. The existing append-only and
monotonic-order contract remains explicit. See
[`INCREMENTAL_RANGE_WINDOW.md`](INCREMENTAL_RANGE_WINDOW.md).
## M065o: Peer-Aware Incremental `RANGE` Distinct Counts

Adopted exact incremental `COUNT(DISTINCT int64)` maintenance for numeric
`RANGE` frames. The active queue provides expiry order and a value-to-
multiplicity map preserves duplicates without rescanning the frame. NULLs are
ignored, peer replacements remain differential, and ascending/descending
partitioned reference tests cover the contract. See
[`INCREMENTAL_RANGE_WINDOW.md`](INCREMENTAL_RANGE_WINDOW.md).
## M065p: Peer-Aware Incremental `RANGE` Averages

Adopted incremental `AVG(int64)` maintenance for numeric RANGE frames by
combining the existing checked sum and valid-count state. NULL values are
ignored, peers receive exact differential replacements, and overflow remains
atomic. Ascending/descending partitioned reference tests cover the behavior.
See [`INCREMENTAL_RANGE_WINDOW.md`](INCREMENTAL_RANGE_WINDOW.md).
## M065q: Peer-Aware Incremental `RANGE` Boundary Values

Adopted bounded numeric RANGE boundary maintenance for generic values. The
FIRST_VALUE path uses an active expiry queue; LAST_VALUE updates only equal-
order peers, preserving exact differential semantics without retaining all
older rows. NULLs, descending order, expiry, and partitioned reference cases
are covered. See [`INCREMENTAL_RANGE_WINDOW.md`](INCREMENTAL_RANGE_WINDOW.md).
## M065r: Peer-Aware Incremental `RANGE` `NTH_VALUE`

Adopted a fixed-position, append-only numeric RANGE NTH_VALUE maintainer. It
retains only the active range plus the current peer group, emits differential
replacements when equal-order peers change the selected position, preserves
NULLs, and validates callbacks and monotonic order atomically. The benchmark
shows a 2.89x CPU improvement versus the pre-change materialized path, with
65.0% higher cumulative allocation bytes and 2.28x fewer allocations. See
[`INCREMENTAL_RANGE_WINDOW.md`](INCREMENTAL_RANGE_WINDOW.md).
| ClickHouse / Materialize | Automatic selection of a compiled native dataflow path | Adopted as a narrow default optimization | Plain scalar `CACHE`/`KEYS` materialized projections over ordinary row resolvers automatically use the existing native batch runtime. Specialized resolver contracts and richer SQL remain on their established paths. `SQLQueryOptions.DisableNativeDataflow` provides an explicit fallback; the paired 4,096-row benchmark is 1.79x faster, uses 3.68x less heap, and uses 3.00x fewer allocations. See [SQL_AUTO_NATIVE_DATAFLOW.md](SQL_AUTO_NATIVE_DATAFLOW.md) and [BENCHMARK.md](BENCHMARK.md#m052p-automatic-native-scalar-dataflow). |
| ClickHouse / Materialize | Automatic selection of native aggregate and distinct operators | Adopted as a narrow default optimization | Global `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` aggregates and one- or two-field `DISTINCT` projections over ordinary row resolvers automatically use the existing native batch runtime. Grouped, ordered, and specialized-resolver paths retain their established executor; `SQLQueryOptions.DisableNativeDataflow` remains the explicit fallback. The paired 4,096-row benchmark is 6.86x faster and 1,106x lower heap for the aggregate shape, and 10.90x faster and 15.53x lower heap for the distinct shape, with 684.50x and 68.46x fewer allocations respectively. See [SQL_AUTO_NATIVE_OPERATORS.md](SQL_AUTO_NATIVE_OPERATORS.md) and [BENCHMARK.md](BENCHMARK.md#m052q-automatic-native-aggregate-and-distinct). |
| ClickHouse / Materialize | Automatic selection of native ordered Top-N operators | Adopted as a narrow default optimization | Finite direct-field `ORDER BY` pages over ordinary row resolvers automatically use the existing native Top-N batch runtime with mixed directions, stable ties, and `LIMIT`/`OFFSET`. Unbounded ordering, `WITH TIES`, richer SQL, and specialized resolver contracts retain their established executor; `SQLQueryOptions.DisableNativeDataflow` remains the explicit fallback. The paired 4,096-row benchmark is 8.19x faster, 35.82x lower heap, and 32.92x lower allocations. See [SQL_AUTO_NATIVE_ORDERED.md](SQL_AUTO_NATIVE_ORDERED.md) and [BENCHMARK.md](BENCHMARK.md#m052r-automatic-native-ordered-top-n). |
| ClickHouse / Materialize | Automatic selection of native grouped aggregation | Adopted as a narrow default optimization | One- and two-field grouped aggregates over ordinary row resolvers automatically use the existing native grouped batch runtime. `HAVING`, ordered or bounded grouped output, richer SQL, and specialized resolver contracts retain their established executor; `SQLQueryOptions.DisableNativeDataflow` remains the explicit fallback. The paired 20,000-row benchmark is 3.21x faster, 3.22x lower heap, and 3.03x lower allocations. See [SQL_AUTO_NATIVE_GROUPED.md](SQL_AUTO_NATIVE_GROUPED.md) and [BENCHMARK.md](BENCHMARK.md#m052s-automatic-native-grouped-aggregation). |
| ClickHouse / Materialize | Automatic grouped `HAVING` plus bounded Top-N fusion | Adopted as a narrow default optimization | One-field grouped aggregates with native-rewritable `HAVING` and finite selected-field ordering over ordinary row resolvers automatically use the existing grouped Top-N batch runtime. Qualified or ambiguous ordering, alias-only unsupported `HAVING`, `WITH TIES`, richer SQL, and specialized resolver contracts retain their established executor; `SQLQueryOptions.DisableNativeDataflow` remains the explicit fallback. The paired 20,000-row benchmark is 4.46x faster, 6.08x lower heap, and 4.91x lower allocations. See [SQL_AUTO_NATIVE_GROUPED_ORDERED.md](SQL_AUTO_NATIVE_GROUPED_ORDERED.md) and [BENCHMARK.md](BENCHMARK.md#m052t-automatic-native-grouped-top-n). |
| ClickHouse / Materialize | Automatic scalar finite-window early termination | Adopted as a narrow default optimization | Plain scalar `LIMIT`/`OFFSET` pages over ordinary row resolvers automatically use the existing early-terminating native batch runtime. `ORDER BY`, `WITH TIES`, richer SQL, and specialized resolver contracts retain their established executor; `SQLQueryOptions.DisableNativeDataflow` remains the explicit fallback. The paired 20,000-row benchmark is 986x faster, 2,677x lower heap, and 1,952x lower allocations. See [SQL_AUTO_NATIVE_SCALAR_LIMIT.md](SQL_AUTO_NATIVE_SCALAR_LIMIT.md) and [BENCHMARK.md](BENCHMARK.md#m052v-automatic-native-scalar-limitoffset). |
| ClickHouse / Materialize | Automatic unordered DISTINCT finite-window early termination | Adopted as a narrow default optimization | One- and two-field direct `DISTINCT` pages over ordinary row resolvers automatically use typed membership state, first-seen `OFFSET`, and early termination at the requested unique `LIMIT`. Ordered distinct, `WITH TIES`, richer SQL, and specialized resolver contracts retain their established executor; `SQLQueryOptions.DisableNativeDataflow` remains the explicit fallback. The paired 20,000-row benchmark is 2,923x faster, 2,721x lower heap, and 4,188x lower allocations. See [SQL_AUTO_NATIVE_DISTINCT_LIMIT.md](SQL_AUTO_NATIVE_DISTINCT_LIMIT.md) and [BENCHMARK.md](BENCHMARK.md#m052w-automatic-native-distinct-limitoffset). |
| ClickHouse / Materialize | Automatic global aggregate finite-window reduction | Adopted as a narrow default optimization | Global aggregate queries with `LIMIT`/`OFFSET` over ordinary row resolvers automatically use the existing one-row native aggregate runtime and apply the window after reduction. `LIMIT 0`, positive offsets, richer SQL, and specialized resolver contracts retain their established executor; `SQLQueryOptions.DisableNativeDataflow` remains the explicit fallback. The paired 20,000-row benchmark is 6.78x faster, 5,405x lower heap, and 3,336x lower allocations. See [SQL_AUTO_NATIVE_AGGREGATE_LIMIT.md](SQL_AUTO_NATIVE_AGGREGATE_LIMIT.md) and [BENCHMARK.md](BENCHMARK.md#m052x-automatic-native-aggregate-limitoffset). |
| ClickHouse / Materialize | Automatic composite grouped bounded Top-N | Adopted as a narrow default optimization | Two-field grouped aggregates with native-rewritable `HAVING`, uniquely resolved selected-field ordering, and finite `LIMIT`/`OFFSET` automatically use composite group state plus a bounded Top-N heap. Qualified, missing, or ambiguous ordering, unsupported `HAVING`, `WITH TIES`, richer SQL, and specialized resolver contracts retain the established executor; `SQLQueryOptions.DisableNativeDataflow` remains the explicit fallback. The paired 20,000-row benchmark is 4.53x faster, 7.16x lower heap, and 40.39x fewer allocations. See [SQL_AUTO_NATIVE_COMPOSITE_GROUPED_ORDERED.md](SQL_AUTO_NATIVE_COMPOSITE_GROUPED_ORDERED.md) and [BENCHMARK.md](BENCHMARK.md#m052y-automatic-native-composite-grouped-top-n). |
| ClickHouse | Packed boolean predicate evaluation | Adopted as a narrow representation-aware optimization | Direct `WHERE` comparisons against validated `ColumnarBoolColumn` bitmaps use a byte-oriented kernel for `=`, `!=`, and `<>`; legacy columns and wider expressions retain the established evaluator. The paired 4,096-row benchmark is 3.12x faster, 2.52x lower heap, and 2.99x lower allocations, with no wire or persistence-format change. See [SQL_PACKED_BOOLEAN_PREDICATE.md](SQL_PACKED_BOOLEAN_PREDICATE.md) and [BENCHMARK.md](BENCHMARK.md#m065t-sql-packed-boolean-predicate-kernel). |
| ClickHouse | Parallel RowBinary decode | Adopted as a bounded automatic optimization | Large RowBinary payloads index row boundaries once and decode independent ranges in parallel while preserving order and the existing wire format. Small inputs, fewer than 256 rows, and single-core processes retain serial decoding. The paired 4,096-row benchmark is 2.16x faster and 2.16x higher throughput for 1.27% more transient bytes and 10 more allocations. See [SQL_PARALLEL_ROW_BINARY.md](SQL_PARALLEL_ROW_BINARY.md) and [BENCHMARK.md](BENCHMARK.md#ch-047-parallel-rowbinary-decode). |
| ClickHouse | C202 partition-affine asynchronous-insert buffers | Adopted as an explicit partitioned API | `hatPipeline.PartitionedAsyncBatcher` gives each caller-selected partition an independent bounded queue and worker, allowing partition handlers to run concurrently while preserving per-partition order. It does not add automatic sharding or routing, and the existing single-worker `AsyncBatcher` remains unchanged. The four-partition benchmark is 3.09x faster for CPU work and keeps timed submission at 0 B/op and 0 allocs/op; construction costs 31,912 B and 58 allocations. See [C202_PARTITIONED_ASYNC_BATCHER.md](C202_PARTITIONED_ASYNC_BATCHER.md) and [BENCHMARK.md](BENCHMARK.md#c202-partition-affine-asynchronous-batching). |
| Tarantool | Transaction and statement timeouts | Adopted as an opt-in transaction boundary | `SQLTransactionOptions.Timeout` bounds the lifetime after snapshot capture, propagates the earlier deadline into relational `Query`, and releases private snapshots, savepoints, and serializable locks on expiry. The zero value keeps existing transactions on the no-clock-check path; per-query `SQLQueryOptions.Timeout` remains separate. See [TR038_TRANSACTION_TIMEOUT.md](TR038_TRANSACTION_TIMEOUT.md) and [BENCHMARK.md](BENCHMARK.md#tr-038-sql-transaction-timeouts). |
| ClickHouse | Map key/value subcolumn pruning | Adopted as a narrow automatic columnar optimization | `ColumnarMapColumn` stores sorted flat key/value vectors and `ColumnarMapSubcolumnSourceResolver` lets a source load only requested one-level JSON object paths. Missing keys, explicit NULLs, and unsupported paths retain established SQL semantics; the measured 10,000-row query is 35.4x faster, uses 15.9x less heap, and uses 2.13x fewer allocations. See [CH030_MAP_SUBCOLUMNS.md](CH030_MAP_SUBCOLUMNS.md) and [BENCHMARK.md](BENCHMARK.md#ch-030-map-keyvalue-subcolumn-pruning). |
| ClickHouse | Durable asynchronous-insert deduplication | Adopted as an opt-in journal-backed async write control | `AsyncInsertBuffer` accepts bounded idempotency keys when `CommandJournalOptions.IdempotencyCapacity` is positive; same-payload retries survive journal replay without appending or reapplying, while conflicting reuse fails. The 64-command unkeyed benchmark stayed at 268 allocations and 82,577 B/op; keyed duplicate retries measured 122,221 ns/op, 105,412 B/op, and 404 allocations. See [CHU01_DURABLE_ASYNC_INSERT_DEDUP.md](CHU01_DURABLE_ASYNC_INSERT_DEDUP.md) and [BENCHMARK.md](BENCHMARK.md#ch-u01-durable-asynchronous-insert-deduplication). |
| ClickHouse | Unified external `ORDER BY` spill | Adopted as an opt-in streaming source capability | `ExternalStreamSourceResolver` lets `ExecuteSQLQueryRows` feed direct `EXTERNAL('name')` sources into the existing bounded external-sort runs. `ExternalTables` preserves immutable replacement snapshots and cancellation cleanup; resolvers without the optional method retain materialized execution. See [CHU02_EXTERNAL_ORDER_SPILL.md](CHU02_EXTERNAL_ORDER_SPILL.md) and [BENCHMARK.md](BENCHMARK.md#ch-u02-external-order-by-spill). |
| ClickHouse | External `DISTINCT` spill | Adopted as an opt-in streaming source capability | Direct `EXTERNAL('name')` `SELECT DISTINCT` queries can feed the existing exact bounded key-run/ordinal merge operator through `ExternalStreamSourceResolver`. First-occurrence order, duplicate elimination, quota cleanup, and the materialized fallback remain intact. See [CHU04_EXTERNAL_DISTINCT_SPILL.md](CHU04_EXTERNAL_DISTINCT_SPILL.md) and [BENCHMARK.md](BENCHMARK.md#ch-u04-external-distinct-spill). |
| ClickHouse | External window-function streaming | Adopted as an opt-in streaming source capability | Direct `EXTERNAL('name')` sources with `ExternalStreamSourceResolver` can stream unpartitioned, unordered `ROW_NUMBER`/rank, running numeric aggregates, fixed-offset `LAG`, and fixed-offset `LEAD` windows with bounded state. Partitioned, explicitly framed, and order-dependent windows retain existing materialization behavior. See [CHU05_EXTERNAL_WINDOW_STREAM.md](CHU05_EXTERNAL_WINDOW_STREAM.md) and [BENCHMARK.md](BENCHMARK.md#ch-u05-external-window-streaming). |
| ClickHouse | Dense integer literal `IN` membership | Adopted as a bounded extension of prepared typed `IN` search | Lists with at least 16 safe-range integer literals and a dense span use a `uint64` bitmap plus compact `int64` fallback values. Sparse, mixed, dynamic, and NULL-containing lists retain existing linear or binary-search paths. The paired 10,000-value benchmark is 3.67x faster and 1.97x smaller in retained representation bytes with zero lookup allocations; preparation is 1.52x slower, so the optimization benefits compiled/reused expressions. See [CHU17_DENSE_INTEGER_IN.md](CHU17_DENSE_INTEGER_IN.md) and [BENCHMARK.md](BENCHMARK.md#ch-u17-dense-integer-in-sets). |
| ClickHouse | Typed scalar JSON subcolumns | Adopted as an opt-in narrow columnar optimization | `ColumnarJSONSubcolumn` stores typed scalar JSON paths with compact payloads plus presence/validity bitmaps. `ColumnarJSONSubcolumnSourceResolver` can serve them for literal `JSON_VALUE`, `JSON_EXISTS`, and `JSON_QUERY` paths; unsupported or unavailable cases preserve the existing map/row path. The measured 4,096-row query is 3.92x faster, uses 4.27x less heap, and uses 2.95x fewer allocations; one-time materialization costs 2.87 ms and 2.59 MB in the fixture. See [CH031_TYPED_JSON_SUBCOLUMNS.md](CH031_TYPED_JSON_SUBCOLUMNS.md) and [BENCHMARK.md](BENCHMARK.md#ch-031-typed-json-subcolumns). |
| ClickHouse | Automatic typed JSON subcolumn promotion | Adopted as an opt-in bounded source primitive | `JSONSubcolumnAutoMaterializer` promotes repeated scalar paths after three observations, keys them by caller-supplied source generation, and bounds entries, rows, and retained typed payload. Cold, mixed-type, oversized, and stale paths fall back without retaining raw documents. Cached `Observe` is 28,925x faster than rematerializing the 4,096-row fixture with 0 B/op and 0 allocs/op; cached batch resolution is 6,589x faster with 752 B/op and 4 allocs/op. See [CH031_AUTOMATIC_TYPED_JSON_SUBCOLUMNS.md](CH031_AUTOMATIC_TYPED_JSON_SUBCOLUMNS.md) and [BENCHMARK.md](BENCHMARK.md#ch-031-automatic-typed-json-subcolumn-promotion). |
| ClickHouse | Variant/JSON subcolumn projection of only referenced paths | Adopted by combining shared path IDs, typed scalar subcolumns, and opt-in generation-safe dynamic promotion; complex or unsupported paths retain the existing fallback | [C248_VARIANT_JSON_SUBCOLUMNS.md](C248_VARIANT_JSON_SUBCOLUMNS.md), [BENCHMARK.md](BENCHMARK.md#c248-variantjson-subcolumn-projection) |
| ClickHouse | Vectorized fixed-width decimal kernels | Adopted as a portable allocation-free execution primitive | `SQLDecimal128`/`SQLDecimal256` compare in 64-bit words, add/subtract with signed overflow detection, and filter into caller-owned packed bitmaps. Existing RowBinary statistics pruning uses the comparison kernel; same-scale coefficient operations preserve explicit caller overflow handling. The five-sample 1,024-value benchmarks improve comparison 1.90x/2.36x, addition 1.27x/1.94x, and packed filtering 1.35x/1.75x for 128/256-bit values with 0 B/op and 0 allocs/op. See [CHU15_VECTORIZED_DECIMAL_KERNELS.md](CHU15_VECTORIZED_DECIMAL_KERNELS.md) and [BENCHMARK.md](BENCHMARK.md#ch-u15-fixed-width-decimal-kernels). |
| ClickHouse | Background prioritized skip-index rebuild queue | Adopted as an importable opt-in maintenance primitive | `hatSql.SQLIndexRebuildQueue` bounds pending work, prioritizes rebuild callbacks, preserves FIFO ties, propagates cancellation, reports monotone progress, and retains bounded terminal history. It has no default workers and no automatic schema mutation; enqueue cost is a measured 405.0 ns/op, 280 B/op, and 3 allocs/op versus a 1.677 ns/op direct callback control. See [CHU12_BACKGROUND_INDEX_REBUILD_QUEUE.md](CHU12_BACKGROUND_INDEX_REBUILD_QUEUE.md) and [BENCHMARK.md](BENCHMARK.md#ch-u12-background-index-rebuild-queue). |
| ClickHouse | Per-index skip-index EXPLAIN diagnostics | Adopted as a diagnostic-only optional resolver contract | `EXPLAIN ANALYZE` reports the selected JSON-path skip index kind/path, bitmap payload bytes, candidate/skipped rows and segments, and exact residual predicate work. Ordinary query results and EXPLAIN plans without diagnostics retain their existing behavior; the nine-sample paired benchmark shows identical ordinary-query median memory/allocations and only `+1,274 B/op` and `+28 allocs/op` on the measured EXPLAIN path. See [CHU49_SKIP_INDEX_EXPLAIN.md](CHU49_SKIP_INDEX_EXPLAIN.md) and [BENCHMARK.md](BENCHMARK.md#ch-u49-skip-index-explain-diagnostics). |

### Materialize MZ-028: Temporal Interval Arrangement

Implemented as the importable `hatSql.SQLTemporalIntervalArrangement`. It
keeps bounded half-open valid-time intervals in per-key augmented treaps,
supports atomic replacement and deletion, returns detached point/range
matches, and preserves deterministic snapshots. The 4,096-interval point
lookup benchmark is 17.2x faster than a linear scan for 19% more temporary
bytes and one additional allocation; the existing `TemporalTable` path is
unchanged. See [MZ028_TEMPORAL_INTERVAL_ARRANGEMENT.md](MZ028_TEMPORAL_INTERVAL_ARRANGEMENT.md)
and [BENCHMARK.md](BENCHMARK.md#mz-028-temporal-interval-arrangement).

### MZ-030: Incremental Differential Join

Implemented in `hatSql` as the imported `IncrementalJoin` API. It maintains
exact signed inner-join deltas for keyed updates and retractions, validates
batches atomically, supports same-key replacement, and exposes deterministic
snapshots. See [MZ030_INCREMENTAL_JOIN.md](MZ030_INCREMENTAL_JOIN.md) for the
API, scope, and measured tradeoffs. SQL planner integration and outer joins
remain open.
### Materialize MZ-29: Incremental Interval-Join Maintenance

Implemented as the imported `hatSql.IncrementalIntervalJoin` API. It maintains
exact signed equi-inner join deltas over half-open validity intervals using
equality buckets and interval pruning, with atomic validation, replacement,
overflow checks, row cloning, and deterministic snapshots. The separate
`ENGINE_IDEAS.md` MZ-029 entry is spillable arrangements and remains open.
See [MZ029_INCREMENTAL_INTERVAL_JOIN.md](MZ029_INCREMENTAL_INTERVAL_JOIN.md)
for the API and measured tradeoffs.
### Materialize MZ-005: Immutable Sealed Upsert Runs

Implemented as the importable `hatDataStructure.SealedUpsertRun`. It
consolidates keyed updates, sorts and front-codes final records, keeps sparse
restart points for bounded lookup, and validates CRC-protected binary round
trips under configurable record, key, value, and wire-size limits. Point lookup
and build are slower than a hot mutable map, so this remains an opt-in format
for persisted, transferred, and compaction input. See
[MZ005_IMMUTABLE_SEALED_UPSERT_RUN.md](MZ005_IMMUTABLE_SEALED_UPSERT_RUN.md) and
[BENCHMARK.md#mz-005-immutable-sealed-upsert-runs](BENCHMARK.md#mz-005-immutable-sealed-upsert-runs).
### ClickHouse CH-045: GLOBAL IN / GLOBAL JOIN Broadcast Planning

Implemented as the importable, caller-owned `hatSql.GlobalJoinBroadcastPlanner`.
It bounds broadcast decisions by rows and bytes, keys reuse by query
fingerprint plus source epoch, caps metadata with FIFO eviction, and reports
remote subquery and fanout accounting. Large results fall back to explicit
per-worker execution; network transport, payload publication, and SQL planner
wiring remain caller-owned. See
[CH045_GLOBAL_JOIN_BROADCAST_PLANNING.md](CH045_GLOBAL_JOIN_BROADCAST_PLANNING.md)
and [BENCHMARK.md#ch-045-global-in-global-join-broadcast-planning](BENCHMARK.md#ch-045-global-in-global-join-broadcast-planning).
### Materialize MZ-029: Spillable Arrangements

Implemented as the imported `hatDataStructure.SpillableArrangement` opt-in
local tier. It bounds retained value payloads, keeps O(1) key metadata in RAM,
uses CRC-protected binary records, enforces optional disk limits, supports
exact cold reads, and compacts stale records explicitly. The measured cost is
10.4x slower cold reads and 12.5% more transient bytes than an equivalent
copy-on-read in-memory lookup; existing defaults are unchanged. Fully
disk-resident indexes and reopen/restore are intentionally still open.
See [MZ029_SPILLABLE_ARRANGEMENT.md](MZ029_SPILLABLE_ARRANGEMENT.md).
### Materialize MZ-031: Skew-Aware Join Exchange

Implemented as the imported `hatSql.SkewAwareJoinExchange` routing policy. It
keeps cold keys single-owner, broadcasts hot build-side rows, spreads hot
probe-side rows by source key, bounds cold frequency tracking, and exposes a
generation fence for caller-controlled rehydration. Routing is allocation-free
but measured 11.4x slower than a bare join-key hash while reducing the tested
maximum worker load 3.4x; automatic planner/executor integration remains open.
See [MZ031_SKEW_AWARE_JOIN_EXCHANGE.md](MZ031_SKEW_AWARE_JOIN_EXCHANGE.md).
### MZ-032: Late-data reclocking

`hatPipeline.LateDataReclock` is an opt-in bounded source-to-processing remap
sidecar. It separates monotone source and processing frontiers, assigns late
events at their arrival frontier, compacts old remap history behind an anchor,
and supports CRC-validated binary snapshots. It does not buffer payloads or
change existing SQL defaults; callers own source watermark inference and
connector integration.
### MZ-033: Timestamp oracle

The existing importable `hatReplication` package provides both a process-local
atomic `TimestampOracle` and a transport-neutral `GlobalTimestampOracle`.
Global reservations observe caller timestamps, allocate contiguous non-
overlapping ranges, fence restarted node epochs and coordinator terms, and make
retries idempotent through per-node sequences. `GlobalTimestampLease` consumes a
grant locally with an atomic counter and no per-timestamp allocation. The
coordinator snapshot is deterministic and validated on restore. Consensus,
replication, and durability remain caller-owned, and existing SQL defaults are
unchanged.

See [MZ033_TIMESTAMP_ORACLE.md](MZ033_TIMESTAMP_ORACLE.md) and the raw
measurements in [BENCHMARK.md](BENCHMARK.md).

### ClickHouse CH-023: Selective partition restore

`RestoreBackupBundle` now accepts an opt-in strict subset of a region-local
partition selector when each selected partition is paired with its declared
key prefix. The extracted snapshot is atomically rewritten to the selected
prefixes and verified again, so the published restore contains only the
requested partition data. Checkpoint-only journal markers are validated and
preserved. A post-snapshot replay tail is also supported for single-key
`SET*`, `INC`, `DEL`, `EXPIRE`, and `EXPIREAT` mutations: selected commands are
applied to the filtered snapshot and the staged journal is atomically advanced
to a checkpoint. Batches, outbox/idempotency-bearing entries, unsupported
complex commands, and Pebble checkpoint/repository subset restores remain
rejected before destination mutation.

See [BENCHMARK.md#ch-064-selective-partition-restore](BENCHMARK.md#ch-064-selective-partition-restore)
for the measured size reduction and restore-cost tradeoff.

### CHG02: Small-cardinality `GROUP BY` index

Implemented as an automatic low-cardinality lookup optimization for streamable
and columnar grouped aggregation. The first four normalized keys use an
inline linear index with no heap allocation; the fifth key promotes the state
to the existing `map[string]int` path. The paired benchmark shows the main
benefit at one and four groups while larger groups retain equivalent
allocation behavior and end-to-end performance within measurement noise. See
[CHG02_SMALL_GROUP_INDEX.md](CHG02_SMALL_GROUP_INDEX.md) and
[BENCHMARK.md#chg02-small-cardinality-group-by-index](BENCHMARK.md#chg02-small-cardinality-group-by-index).
## C204: Projection Idempotency Propagation

| Source | Adopted idea | Implementation | Evidence |
| --- | --- | --- | --- |
| ClickHouse | Preserve async-insert identity through dependent materialized views | Journal idempotency keys are carried by `SQLJournalProjectionRunner`, `ProjectionRun`, and `MaterializedViewStatus` | [C204_PROJECTION_IDEMPOTENCY.md](C204_PROJECTION_IDEMPOTENCY.md), [BENCHMARK.md](BENCHMARK.md#c204-projection-idempotency-metadata) |

## C250: Retry-Safe Async Insert Identities

| Source | Adopted idea | Implementation | Evidence |
| --- | --- | --- | --- |
| ClickHouse | Keep a retry identity with an asynchronous insert so retries do not apply the same payload twice | Public opt-in `hatPipeline.AsyncInsertDeduplicator` keys a bounded `source`/`ID` ledger, detects payload conflicts, and can persist CRC-protected records with `AsyncInsertDedupFileStore` | [C250_ASYNC_INSERT_IDENTITIES.md](C250_ASYNC_INSERT_IDENTITIES.md), [BENCHMARK.md](BENCHMARK.md#c250-retry-safe-async-insert-identities) |

C250 is distinct from C204: C204 propagates identity metadata through
projections, while C250 performs the admission decision at the ingestion
boundary. Existing unkeyed async callers remain unchanged.

## C205: Subquery Result Cache Controls

| Source | Adopted idea | Implementation | Evidence |
| --- | --- | --- | --- |
| ClickHouse / Materialize | Reuse stable internal query fragments without requiring a whole-query materialized result | Importable `SQLQueryOptions.SubqueryResultCache` reuses eligible uncorrelated derived queries, non-recursive CTE bodies, and UNION/INTERSECT/EXCEPT branches. Source versions, parameters, collation, schema version, plan mode, and settings fingerprint namespace entries; row results are cloned. The option is off by default and excludes correlated, lateral, recursive, volatile, custom-function, and unversioned paths. | [C205_SUBQUERY_RESULT_CACHE.md](C205_SUBQUERY_RESULT_CACHE.md), [BENCHMARK.md](BENCHMARK.md#c205-subquery-result-cache) |

## C227: External Group-Merge Memory Budget

| Source | Adopted idea | Implementation | Evidence |
| --- | --- | --- | --- |
| ClickHouse | Bound external aggregation memory during run merging | `SQLQueryOptions.MaxGroupMergeBytes` bounds the estimated decoded current record frontier across active external `GROUP BY` spill readers. Zero keeps the guard off; negative values are rejected; over-budget merges return a deterministic error and clean all temporary files. The estimator is allocation-free and does not claim to cap total process RSS. | [C227_GROUP_MERGE_BUDGET.md](C227_GROUP_MERGE_BUDGET.md), [BENCHMARK.md](BENCHMARK.md#c227-external-group-merge-memory-budget) |

## Materialize M-U01: Durable Connector Lifecycle State

`hatPipeline.ConnectorRegistry` now exposes detached lifecycle checkpoints and
the deterministic, bounded, CRC32C-protected `HCS1` binary format through
`SnapshotState`, `MarshalSnapshot`, `UnmarshalConnectorRegistrySnapshot`, and
`NewConnectorRegistryFromSnapshot`. Restore requires an exact map of fresh
connector implementations, never invokes connector callbacks, and leaves
restart reconciliation explicit to the caller. The existing status-only
snapshot API and all defaults are unchanged. The 64-connector fixture encodes
4,331 bytes versus 18,049 bytes for JSON; binary codec medians are 7.90x faster
to encode and 15.66x faster to decode. See
[MU01_DURABLE_CONNECTOR_STATE.md](MU01_DURABLE_CONNECTOR_STATE.md) and
[BENCHMARK.md](BENCHMARK.md#m-u01-durable-connector-lifecycle-state).

## CH-004: `FINAL` Read Semantics

ClickHouse-style query-time `FINAL` is adopted as an explicit opt-in SQL
capability. `SQLFinalReplacing` keeps the highest caller-defined version per
key, while `SQLFinalCollapsing` cancels caller-defined opposite signs and
preserves unmatched rows. The resolver contract is required and the executor
fails closed when a marked source has no configuration. Existing query paths
remain unchanged without `FINAL`; marked queries bypass shortcuts that could
return unreconciled rows and reconcile before downstream operators consume the
source. See [CH004_FINAL_READ.md](CH004_FINAL_READ.md) and
[BENCHMARK.md](BENCHMARK.md#ch-004-final-read-semantics). Persistent
schema-bound metadata and background merge integration remain open.

## CH-005: Delete Bitmap State Snapshots

ClickHouse-style lightweight delete state is partially adopted for typed-table
patch parts. `TypedTable.MarshalPatchState` and `RestorePatchState` persist and
restore the packed physical-row bitmap with a bounded versioned CRC format,
exact key-order validation, and atomic failure behavior. The default delete,
read, and compaction paths are unchanged. Full immutable stored-part manifests
and automatic cross-process part hydration remain open. See
[CH005_DELETE_BITMAP_SNAPSHOT.md](CH005_DELETE_BITMAP_SNAPSHOT.md) and
[BENCHMARK.md](BENCHMARK.md#ch-005-delete-bitmap-state-snapshots).
## CH-006: Durable Mutation Dependency Queue

CH-006 is partially adopted through the importable
`hatSql.SQLMutationDependencyQueue`. It wraps the existing dependency graph
with a bounded binary write-ahead log, CRC validation, monotone record
sequences, crash-tail truncation, replay, and caller-triggered atomic
compaction. Add, claim, complete, fail, retry, and requeue transitions are
synced before returning. The default in-memory graph and SQL execution path are
unchanged; automatic `ALTER`/`DELETE` planner integration and cross-process
leases remain open. See [CH006_DURABLE_MUTATION_QUEUE.md](CH006_DURABLE_MUTATION_QUEUE.md)
and [BENCHMARK.md](BENCHMARK.md#ch-006-durable-mutation-dependency-queue).

## CH-008: Column TTL

CH-008 is partially adopted through importable per-column TTL metadata on
`TypedTableColumn`. Processing-time and event-time policies mask expired values
as SQL `NULL` in live row/columnar reads, statistics, and histograms while
retaining the row. `PurgeExpiredColumns` and the opt-in TTL scheduler physically
clear expired values and emit exact `UPDATE` changes. Processing-time column
deadlines have a bounded CRC-protected `MarshalColumnTTLState` /
`RestoreColumnTTLState` pair for backup and restore. Automatic schema inference
and live-clock mutation of historical MVCC snapshots remain intentionally out
of scope. See [CH008_COLUMN_TTL.md](CH008_COLUMN_TTL.md) and
[BENCHMARK.md](BENCHMARK.md#ch-008-column-ttl).

## CH-009: TTL Rollup

CH-009 is partially adopted through importable `hatSql.TypedTableTTLRollup`.
It reuses the exact typed-table aggregate engine to summarize only row-TTL
`DELETE` before-images after detail expiry. `RegisterWithRollup` connects the
summary to the existing shared TTL scheduler as an explicit opt-in; ordinary
registration, background startup, live-table reads, and column-TTL behavior
remain unchanged. Rollup persistence and automatic restart replay remain
caller-owned. See [CH009_TTL_ROLLUP.md](CH009_TTL_ROLLUP.md) and
[BENCHMARK.md](BENCHMARK.md#ch-009-ttl-rollup).

## CH-010: Materialized and Default Columns

CH-010 is partially adopted through `TypedTableColumn.GeneratedMode` and
`GeneratedDependencies`. The zero-value materialized mode preserves the
existing callback behavior; the opt-in default mode preserves valid caller
values and computes only null values. Dependency names are validated once at
schema construction, generated callbacks run in cached topological order, and
the explicit-default path avoids a row clone. SQL expression parsing, DDL
integration, and persistence of generated expressions remain caller-owned. See
[CH010_MATERIALIZED_DEFAULT_COLUMNS.md](CH010_MATERIALIZED_DEFAULT_COLUMNS.md)
and [BENCHMARK.md](BENCHMARK.md#ch-010-materialized-and-default-columns).

## CH-011: Projection DDL

CH-011 is partially adopted through session-local `CREATE PROJECTION`, `DROP
PROJECTION`, and `REFRESH PROJECTION`. The implementation reuses
`MaterializedViews`, requires source versions before creation, selects only
exact fresh query matches, and falls back to the source path on version
mismatch. Durable table-bound projection metadata, automatic source-write
notifications, and cross-node coordination remain caller-owned. See
[CH011_PROJECTION_DDL.md](CH011_PROJECTION_DDL.md) and
[BENCHMARK.md](BENCHMARK.md#ch-011-projection-ddl).

## CH-012: Projection Advisor Cost Model

CH-012 is partially adopted through the opt-in
`SQLProjectionAdvisor.CostBasedRecommendations` API. It combines bounded
observed average query latency with caller-supplied expected query volume,
projection-hit latency, initial build cost, and refresh cost. Saturating
arithmetic prevents duration overflow, while automatic forecasting, planner
wiring, and persistent workload history remain caller-owned. See
[CH012_PROJECTION_ADVISOR_COST.md](CH012_PROJECTION_ADVISOR_COST.md) and
[BENCHMARK.md](BENCHMARK.md#ch-012-projection-advisor-cost).

## MZ-024: Automatic Arrangement Key Selection

MZ-024 is partially adopted through `RecommendSQLArrangement` and the
diagnostic integration in `EXPLAIN`. Existing arrangement metadata can include
explicit fields; the bounded selector scores matches across filter, grouping,
ordering, and join workloads and marks one deterministic recommendation. It
never creates or changes state, and automatic planner rewrites remain
caller-owned. See [MZ024_ARRANGEMENT_KEY_SELECTION.md](MZ024_ARRANGEMENT_KEY_SELECTION.md)
and [BENCHMARK.md](BENCHMARK.md#mz-024-automatic-arrangement-key-selection).

## TT-049: SQL Row Lock Leases

TT-049 is partially adopted as the importable bounded
`SQLRowLockManager`. It provides per-key exclusive leases, context-aware
waiting, nonblocking `TryAcquire`, explicit capacity errors, sharded lookup,
idle-key reclamation, and idempotent release. The manager is process-local;
SQL `FOR UPDATE` parsing, transaction lifetime, durable recovery, and
distributed fencing remain caller-owned. See [TT049_ROW_LOCK_LEASES.md](TT049_ROW_LOCK_LEASES.md)
and [BENCHMARK.md](BENCHMARK.md#tt-049-sql-row-lock-leases).
## Tarantool-Inspired Transport Reliability

- **TR-045 connection circuit breaker and health scoring:** `hatPeer.CompactPeerCircuitBreaker` is an opt-in wrapper with bounded failure admission, one half-open probe, caller-cancellation exclusion, and a local health score. See [TR045_COMPACT_PEER_CIRCUIT_BREAKER.md](TR045_COMPACT_PEER_CIRCUIT_BREAKER.md).

## TR-007: Adaptive WAL Group Commit

TR-007 is adopted as the opt-in `hatJournal.Options.AdaptiveGroupCommit`
policy. It shortens the existing collection window when queued writers show
pressure while preserving ordered append, sync-before-apply/ack, rollback, and
replay behavior. The zero value remains the fixed-window implementation. In a
16-concurrent-writer benchmark it was 1.12x faster with unchanged sync count,
bytes, and allocations. See [TR007_ADAPTIVE_WAL_GROUP_COMMIT.md](TR007_ADAPTIVE_WAL_GROUP_COMMIT.md)
and [BENCHMARK.md](BENCHMARK.md#tr-007-adaptive-wal-group-commit).

## T-U10: Journal-Wide Synchronous Write Quorum

T-U10 is adopted as the opt-in `hatReplication.JournalWriteQuorum` contract.
It binds an exact journal sequence and fence token to a configured voter
quorum, validates duplicate/unknown/stale acknowledgements, and collects
transport callbacks concurrently. `Required: 0` selects a strict majority;
the zero-value configuration is disabled and leaves asynchronous replication
unchanged. Journal append/fsync, transport deadlines, retry idempotency,
fence persistence, and failed-replica repair remain caller-owned. See
[TU10_JOURNAL_WRITE_QUORUM.md](TU10_JOURNAL_WRITE_QUORUM.md) and
[BENCHMARK.md](BENCHMARK.md#t-u10-journal-wide-synchronous-write-quorum).

## T-U13: Durable Cluster Membership

T-U13 is adopted as the opt-in `hatTopology.MembershipJournal`. It persists
generation/fence-checked join and leave records through a CRC-protected `HMM1`
snapshot, retries unchanged operation IDs idempotently, bounds retained history,
and reports gaps instead of replaying incomplete membership changes. Consensus
authorization, cross-node quorum, backup placement, and transport integration
remain caller-owned; the existing topology and replication defaults are
unchanged. See [TU13_DURABLE_CLUSTER_MEMBERSHIP.md](TU13_DURABLE_CLUSTER_MEMBERSHIP.md)
and [BENCHMARK.md](BENCHMARK.md#t-u13-durable-cluster-membership).

## T-U36: Snapshot Rotation Policy

T-U36 adds an opt-in `BackupRotationPolicy` and
`CreateIncrementalBackupRepositoryIfDue`. It combines maximum elapsed time,
journal sequence deltas, and a minimum interval for change-driven rotations.
Incremental repositories can also retain by count and approximate unique
object bytes. The first backup is immediate; a not-due call is read-only, and
existing mutation and backup defaults remain unchanged. See
[TU36_SNAPSHOT_ROTATION.md](TU36_SNAPSHOT_ROTATION.md) and
[BENCHMARK.md](BENCHMARK.md#tu36-snapshot-rotation-policy).

## T-U37: Replica Applier Throttling

Added an opt-in bounded `hatReplication.ApplierThrottle` and wired it to
ordered HTTP journal pulls and gRPC replication streams. The default nil path
keeps existing replay behavior; configured callers trade replica catch-up
throughput for bounded foreground impact. See
[TU37_REPLICA_APPLIER_THROTTLE.md](TU37_REPLICA_APPLIER_THROTTLE.md) and
[BENCHMARK.md](BENCHMARK.md#tu37-replica-applier-throttling).

## M033: Batched Logical Timestamp Oracle

M033 is partially adopted through the opt-in
`hatSql.SQLLogicalTimestampOracle`. It provides monotone process-local
timestamps, external frontier observation, overflow-safe allocation, and a
contiguous `Reserve(count)` path that assigns one source batch with one atomic
update. Distributed uniqueness, uncertainty bounds, persistence, and consensus
remain caller-owned. A 1,024-timestamp reservation was about 810x faster than
1,024 individual atomic increments with zero allocations in the recorded
fixture. See [M033_LOGICAL_TIMESTAMP_ORACLE.md](M033_LOGICAL_TIMESTAMP_ORACLE.md)
and [BENCHMARK.md](BENCHMARK.md#m033-batched-logical-timestamp-oracle).

## Materialize M-U37: Per-Arrangement Compaction Diagnostics

Adopted as opt-in `hatStorage.CompactionDiagnostics`: bounded arrangement registration, allocation-free post-registration recording, deterministic detached snapshots, and fixed-depth history retain logical/physical bytes and caller-defined compaction debt without adding default-path overhead. The storage engine still owns the meaning of debt and the wiring point. See [MU037_COMPACTION_DIAGNOSTICS.md](MU037_COMPACTION_DIAGNOSTICS.md) and [BENCHMARK.md](BENCHMARK.md#m-u37-per-arrangement-compaction-diagnostics).

## Materialize M-U41: Webhook Event Idempotency

Adopted as opt-in `hatSql.WebhookEventDeduplicator`: bounded source/event-ID admission, payload-fingerprint conflict detection, expiry, deterministic CRC-protected HWE1 snapshots, and atomic restore. The caller owns application, durable storage, acknowledgement, and HTTP/webhook wiring; existing defaults remain unchanged. See [MU041_WEBHOOK_IDEMPOTENCY.md](MU041_WEBHOOK_IDEMPOTENCY.md) and [BENCHMARK.md](BENCHMARK.md#m-u41-webhook-event-idempotency).

## ClickHouse CH-G42: Per-Operator SQL Memory Tracking

Adopted as opt-in `hatSql.SQLOperatorMemoryTracker`: bounded peak working-set
observations and typed per-operator admission errors for `GROUP BY`, `SORT`,
and non-`ALL` set operators. The nil option preserves the default path. See
[CHG42_OPERATOR_MEMORY.md](CHG42_OPERATOR_MEMORY.md) and its measured
diagnostic cost.
## T-U39: Named Space Changefeed

T-U39 is adopted as opt-in `hatReplication.SpaceChangefeed`. It provides a
bounded named-space event ring with immutable schema identity, monotone
sequences, replay checkpoints, payload-copy isolation, context cancellation,
bounded subscriber count, and explicit subscriber overflow. It does not alter
existing journal or SQL subscription defaults; durable storage, transport,
authentication, and recovery orchestration remain caller-owned. See
[TU39_SPACE_CHANGEFEED.md](TU39_SPACE_CHANGEFEED.md).

## T-U28: Connection Pool Lifecycle Integration

Adopted the opt-in `hatPeer.ConnectionPool` lifecycle integration. Successful
dials emit `connected`, physical closes emit `disconnected`, and shutdown is
emitted once after active handlers drain. Reused idle connections do not
create duplicate events, and peer/error metadata is bounded before history
retention. The nil registry preserves the existing default path. See
[TU28_CONNECTION_POOL_LIFECYCLE.md](TU28_CONNECTION_POOL_LIFECYCLE.md) and
[BENCHMARK.md](BENCHMARK.md#t-u28-connection-pool-lifecycle).

## M065v: Mutable Incremental Offset Windows

Adopted the opt-in `hatSql.MutableIncrementalOffsetWindow` for exact mutable
`LAG`/`LEAD` differentials. It validates a complete mutation batch before
publishing state, skips sorting for same-position updates, and rebuilds only
affected partitions for structural changes. The existing append-only window
constructor remains the default. See
[INCREMENTAL_MUTABLE_OFFSET_WINDOW.md](INCREMENTAL_MUTABLE_OFFSET_WINDOW.md)
and [BENCHMARK.md](BENCHMARK.md#m065v-mutable-incremental-offset-windows).

## M065w: Mutable Numeric RANGE Windows

Adopted the opt-in `hatSql.MutableIncrementalRangeWindow` for exact mutable
numeric RANGE differentials across the existing aggregate kinds. It validates
complete mutation batches, rebuilds only affected partitions, and uses checked
same-position delta updates for `COUNT` and `SUM(int64)`. The append-only
constructor remains the default. See
[INCREMENTAL_MUTABLE_RANGE_WINDOW.md](INCREMENTAL_MUTABLE_RANGE_WINDOW.md)
and [BENCHMARK.md](BENCHMARK.md#m065w-mutable-numeric-range-windows).

## M065x: Mutable RANGE Boundary Windows

Adopted the opt-in `hatSql.MutableIncrementalRangeBoundaryWindow` for exact
mutable `FIRST_VALUE`/`LAST_VALUE` differentials. It validates complete
mutation batches, rebuilds only affected partitions for structural changes,
and uses an ordered same-position frame scan without cloning the full state.
The existing append-only constructor and default behavior remain unchanged.
See [INCREMENTAL_MUTABLE_RANGE_BOUNDARY_WINDOW.md](INCREMENTAL_MUTABLE_RANGE_BOUNDARY_WINDOW.md)
and [BENCHMARK.md](BENCHMARK.md#m065x-mutable-range-boundary-windows).

## M065y: Mutable RANGE NTH_VALUE Windows

Adopted the opt-in `hatSql.MutableIncrementalRangeNthValueWindow` for exact
mutable `NTH_VALUE` differentials. It validates complete mutation batches,
rebuilds only affected partitions for structural changes, and uses a
peer-aware same-position frame scan for a fixed position. The existing
append-only constructor and default behavior remain unchanged. See
[INCREMENTAL_MUTABLE_RANGE_NTH_VALUE_WINDOW.md](INCREMENTAL_MUTABLE_RANGE_NTH_VALUE_WINDOW.md)
and [BENCHMARK.md](BENCHMARK.md#m065y-mutable-range-nth_value-windows).

## M065z: Mutable Rank Arrangement Fast Path

Adopted a narrow Materialize-style arrangement optimization for
`NewMutableIncrementalRankWindow`. A single update that retains its partition
and order key now reuses the existing rank output and updates only the changed
row's retained maps. Position-changing, partition-changing, and multi-row
mutations continue through the existing affected-partition rebuild, while the
append-only constructor remains unchanged. The targeted benchmark is
`173.7x` faster, `173.6x` lower in transient bytes, and `64.1x` lower in
allocations; the general position-changing benchmark remains neutral. See
[INCREMENTAL_RANK_WINDOW.md](INCREMENTAL_RANK_WINDOW.md#same-position-mutable-update-fast-path)
and [BENCHMARK.md](BENCHMARK.md#m065z-mutable-rank-arrangement-fast-path).

## M065aa: Batched Mutable Rank Arrangement Fast Path

Extended the Materialize-style rank arrangement fast path to mutation batches
containing only same-position updates. The implementation validates every
partition/order callback before publishing, updates retained rows in stable
key order, and emits only the changed signed pairs. Structural batches retain
the affected-partition rebuild, and the append-only constructor remains
unchanged. The eight-update benchmark is `19.9x` faster, `21.5x` lower in
transient bytes, and `7.9x` lower in allocations. See
[INCREMENTAL_RANK_WINDOW.md](INCREMENTAL_RANK_WINDOW.md#batched-same-position-mutable-updates)
and [BENCHMARK.md](BENCHMARK.md#m065aa-batched-mutable-rank-arrangement-fast-path).

## M065ab: Batched Mutable Numeric RANGE SUM Fast Path

Extended the Materialize-style mutable numeric `RANGE SUM(int64)` fast path to
same-position mutation batches. The implementation validates every update,
copies each affected partition once, and applies checked old/new value deltas
only to frames containing the changed order. Structural changes and NULL-state
transitions retain the existing rebuild fallback; the append-only constructor
remains unchanged. The two-update benchmark is `24.1x` faster, uses `18.5x`
fewer transient bytes, and uses `32.6x` fewer allocations. See
[INCREMENTAL_MUTABLE_RANGE_WINDOW.md](INCREMENTAL_MUTABLE_RANGE_WINDOW.md)
and [BENCHMARK.md](BENCHMARK.md#m065ab-batched-mutable-numeric-range-sum-fast-path).

## M065ac: Batched Mutable RANGE Boundary Fast Path

Extended the Materialize-style mutable arrangement optimization to batches of
`FIRST_VALUE`/`LAST_VALUE` updates that retain each row's partition and order
position. The implementation validates every update, evaluates the affected
ordered frame once, and publishes only changed differentials. Structural,
mixed-operation, duplicate-key, and cross-partition batches retain the
existing affected-partition rebuild path; the append-only constructor remains
unchanged. The two-update benchmark is `8.26x` to `8.36x` faster, uses about
`15.2x` fewer transient bytes, and uses `811x` to `1,390x` fewer allocations.
See [INCREMENTAL_MUTABLE_RANGE_BOUNDARY_WINDOW.md](INCREMENTAL_MUTABLE_RANGE_BOUNDARY_WINDOW.md)
and [BENCHMARK.md](BENCHMARK.md#m065ac-batched-mutable-range-boundary-fast-path).

## M065ad: Batched Mutable RANGE NTH_VALUE Fast Path

Extended the Materialize-style mutable arrangement optimization to batches of
`NTH_VALUE` updates that retain one partition and each row's order position.
The implementation validates every replacement, evaluates the peer-aware
frame once, and publishes only changed differentials. Structural,
cross-partition, duplicate-key, and mixed-operation batches retain the
existing affected-partition rebuild path; the append-only constructor remains
unchanged. The two-update benchmark is `8.17x` faster, uses `15.7x` fewer
transient bytes, and uses `835x` fewer allocations. See
[INCREMENTAL_MUTABLE_RANGE_NTH_VALUE_WINDOW.md](INCREMENTAL_MUTABLE_RANGE_NTH_VALUE_WINDOW.md)
and [BENCHMARK.md](BENCHMARK.md#m065ad-batched-mutable-range-nth_value-fast-path).

## M065ae: Batched Mutable RANGE Extrema Fast Path

Extended the Materialize-style mutable arrangement optimization to same-position
`MIN(int64)` and `MAX(int64)` update batches. The implementation validates all
updates, evaluates the affected peer-aware frames with a NULL-aware monotonic
deque, and publishes only changed differentials. Structural, cross-partition,
duplicate-key, and invalid-value batches retain the existing rebuild/error
path; the append-only constructor remains unchanged. The two-update benchmark
is `13.13x` to `13.78x` faster, uses `13.8x` to `14.6x` fewer transient bytes,
and uses `9.9x` to `19.8x` fewer allocations. See
[INCREMENTAL_MUTABLE_RANGE_WINDOW.md](INCREMENTAL_MUTABLE_RANGE_WINDOW.md)
and [BENCHMARK.md](BENCHMARK.md#m065ae-batched-mutable-range-extrema-fast-path).

## M065af: Batched Mutable RANGE Aggregate Fast Path

Extended the Materialize-style mutable arrangement optimization to same-position
`COUNT(DISTINCT int64)` and `AVG(int64)` update batches. Mutable entries retain
validated numeric values for these opt-in kinds, allowing only frames that
contain changed order positions to be recomputed. NULL handling, exact distinct
membership, checked average sums, peer semantics, and atomic invalid-value
validation remain unchanged. Structural, cross-partition, duplicate-key, and
mixed-operation batches retain the existing affected-partition rebuild path.
The matched 2,000-row benchmark measured `4.70x` to `6.32x` lower CPU time,
`11.28x` to `11.36x` lower transient heap, and `17.54x` to `17.87x` fewer
allocations. See
[INCREMENTAL_MUTABLE_RANGE_WINDOW.md](INCREMENTAL_MUTABLE_RANGE_WINDOW.md)
and [BENCHMARK.md](BENCHMARK.md#m065af-batched-mutable-range-aggregate-fast-path).

## M037i: Signed Differential Grouped SUM(DISTINCT)

Adopted a narrow Materialize-style negative-diff and ClickHouse-style distinct
aggregate primitive: `hatSql.GroupSumDistinctInt64DifferentialRows` maintains
exact per-group value multiplicities and a checked distinct sum. Duplicate
weights are preserved, only visible aggregate transitions are emitted, and
invalid total/per-value multiplicity, callback, and overflow errors return no
partial output. The matched 5,120-update benchmark was `1.33x` faster than a
full per-update group rebuild with `1.01x` heap and `1.02x` allocations. The
API is importable and opt-in; existing SQL planner behavior is unchanged. See
[DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md).

## M037j: Signed Differential Grouped COUNT(DISTINCT)

Adopted the adjacent Materialize-style negative-diff and ClickHouse-style
distinct aggregate primitive `hatSql.GroupCountDistinctInt64DifferentialRows`.
It retains exact per-group value multiplicities, suppresses duplicate-only
changes, emits the existing `count_distinct` field, and returns no partial
output for invalid multiplicity, callback, or overflow errors. The matched
5,120-update benchmark was `1.43x` faster than a full rebuild with `1.01x`
heap and `1.02x` allocations. The API is importable and opt-in; existing SQL
planner behavior is unchanged. See
[DIFFERENTIAL_GROUP_BY.md](DIFFERENTIAL_GROUP_BY.md).

## C243: Remote-Part Read-Through Cache

The existing `hatStorage.RemotePartCache` adopts ClickHouse-style immutable
remote-part reuse. Its identity includes normalized object URI, checksum, and
declared size; bounded bytes and entries, single-flight misses, priority-aware
eviction, pinned handles, invalidation, and bounded prefetch keep memory and
remote-read behavior predictable. A measured SHA-256-on-miss validation variant
was rejected because it made 64 KiB cold loads about 3.6x slower without
changing allocations; loaders remain responsible for payload validation when
needed. See [C243_REMOTE_PART_CACHE.md](C243_REMOTE_PART_CACHE.md),
[REMOTE_PART_CACHE.md](REMOTE_PART_CACHE.md), and
[BENCHMARK.md](BENCHMARK.md#c243-remote-part-read-through-cache).

## C242: Bounded Parallel Restore

Adopted a ClickHouse-style independent-part restore path for content-addressed
incremental repositories. `MaxPartConcurrency` is opt-in and bounded at 256;
the default remains serial, gzip bundles remain serial, and resume restores
retain their existing verified-file reuse semantics. Each part is copied with
exclusive destination creation plus size and SHA-256 verification before the
existing atomic publish. See [C242_PARALLEL_RESTORE.md](C242_PARALLEL_RESTORE.md)
and [BENCHMARK.md](BENCHMARK.md#c242-bounded-parallel-restore).
## C244: Local Cache Reuse Validation

ClickHouse-style immutable-part metadata is available through the opt-in
`hatMerkle.PartManifest`. A manifest combines a whole-part checksum with
independent named column checksums. `Validate` is used when bytes enter a
local cache or cross an untrusted boundary; `Equal` is an allocation-free
metadata check for repeated reuse. See [C244_LOCAL_CACHE_REUSE.md](C244_LOCAL_CACHE_REUSE.md)
 and [BENCHMARK.md](BENCHMARK.md#c244-local-cache-reuse-validation) for the
measured validation cost and the fast reuse path.

## TT-018: Page-index Residency Policy

Tarantool/Vinyl-style bounded page-index residency is adopted as the opt-in
generic `hatDataStructure.PageIndexResidency[K,V]`. It retains caller-owned
immutable indexes under byte and entry limits, evicts least-recently-used
entries, keeps `Get` allocation-free, and exposes hit/miss/admission/
replacement/eviction/rejection counters. Existing storage defaults and index
integration remain unchanged; callers own byte charging and invalidation. See
[TT018_PAGE_INDEX_RESIDENCY.md](TT018_PAGE_INDEX_RESIDENCY.md) and
[BENCHMARK.md](BENCHMARK.md#tt-018-page-index-residency-policy).

## TT-021: Packed R-tree Spatial Index

Tarantool-style RTREE support is adopted as the opt-in immutable
`hatDataStructure.PackedRTree[T]`. It bulk-builds spatially coherent leaves,
uses compact integer node ranges, validates finite inclusive boxes, copies
caller input, and provides allocation-free `QueryInto` and `Visit` paths.
Mutable updates, SQL planner integration, persistence, and replication remain
deferred. See [TT021_PACKED_RTREE.md](TT021_PACKED_RTREE.md) and
[BENCHMARK.md](BENCHMARK.md#tt-021-packed-r-tree-spatial-index).

## C237: Projection Selection Explain Output

ClickHouse-style projection diagnostics are available for opt-in non-pipeline
`EXPLAIN`. The plan reports exact candidate selection, stale-source rejection,
logical source/projection bytes, and estimated bytes saved. The implementation
avoids cloning retained projection rows and does not change normal query
execution. See [CH237_PROJECTION_EXPLAIN.md](CH237_PROJECTION_EXPLAIN.md) and
[BENCHMARK.md](BENCHMARK.md#c237-projection-selection-explain-output).
## C233: Per-Query CPU-Time Budgets

Implemented as opt-in cooperative `SQLQueryOptions.MaxCPUTime`. Linux measures
user and system CPU using `getrusage(RUSAGE_THREAD)` and aggregates deltas from
participating threads. `CPUTimeCheckEvery` defaults to 64 checkpoints to bound
syscall overhead; strict every-checkpoint sampling remains configurable. Other
platforms fail explicitly with `ErrSQLCPUTimeUnsupported` rather than treating
wall time as CPU time. See [C233_CPU_TIME_BUDGET.md](C233_CPU_TIME_BUDGET.md).
