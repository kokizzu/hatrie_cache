# New Product Inspiration Backlog

This file contains 50 additional, currently-unadopted candidates for each
reference product. It is separate from INSPIRATION.md, whose large checklist
records capabilities already implemented in this repository. An entry is a
gap candidate only when the specific behavior is not already present; an
adjacent primitive does not count as completion.

Status values:

- [ ] candidate, not implemented
- [x] implemented in this goal and linked to its tests and benchmark
- [-] rejected after a correctness or tradeoff measurement

Every accepted item must start with a focused failing test, preserve the
default-off or backward-compatible behavior where appropriate, and include a
before/after measurement when its motivation is performance.

## ClickHouse (50 candidates)

| ID | Candidate gap | Intended value | Main cost or risk | State |
| --- | --- | --- | --- | --- |
| CH-01 | Named settings profiles with inheritance and validation | Repeatable per-tenant query policy | Configuration complexity | [x] Implemented by `hat/hatSql.SQLNamedSettingsProfile`, bounded parent resolution, and optional `SQLNamedSettingValidator`; see [SQL_NAMED_SETTINGS.md](SQL_NAMED_SETTINGS.md) and [BENCHMARK.md](BENCHMARK.md#ch-001-named-settings-profile-inheritance-and-validation). |
| CH-02 | Include the effective settings fingerprint in result-cache keys | Prevent unsafe cache reuse | Larger keys and invalidation surface | [x] Implemented as the opt-in `SQLQueryOptions.ResultCacheSettingsFingerprint`; see [C208_RESULT_CACHE_METRICS.md](C208_RESULT_CACHE_METRICS.md) and [BENCHMARK.md#ch-002-settings-aware-result-cache-keys](BENCHMARK.md#ch-002-settings-aware-result-cache-keys). |
| CH-03 | Soft and hard resource profiles with a dry-run admission result | Make workload limits explainable | More admission branches | [x] Implemented by `hat/hatSql.NamespaceResourceProfile`, `NewNamespaceQueryGovernorWithProfiles`, and `NamespaceQueryGovernor.DryRun`; see [SQL_NAMESPACE_ADMISSION.md](SQL_NAMESPACE_ADMISSION.md) and [BENCHMARK.md](BENCHMARK.md#ch-003-soft-and-hard-namespace-admission-profiles). |
| CH-04 | Retained query-log tables with time and size rotation | Diagnose historical workload regressions | Disk usage and PII handling | [x] Implemented by optional size/age rotation and bounded numbered archives in `hat/hatSql.SQLQueryLog`; see [SQL_QUERY_LOG.md](SQL_QUERY_LOG.md) and [BENCHMARK.md](BENCHMARK.md#ch-004-retained-query-log-rotation). |
| CH-05 | Retained part and merge event log | Explain compaction and read amplification | Event volume | [x] Implemented as the opt-in bounded `TypedTableStorageEvent` ring; see [TYPED_TABLE_STORAGE_EVENTS.md](TYPED_TABLE_STORAGE_EVENTS.md) and [BENCHMARK.md](BENCHMARK.md#ch-005-typed-table-storage-events). |
| CH-06 | LRU sparse-mark cache separate from data-part cache | Avoid repeated mark reads on hot ranges | Cache memory and invalidation | [x] Implemented as the opt-in `SparsePrimaryMarkCache` for typed-table ordered range metadata; see [TYPED_TABLE_SPARSE_MARK_CACHE.md](TYPED_TABLE_SPARSE_MARK_CACHE.md) and [BENCHMARK.md](BENCHMARK.md#ch-006-lru-sparse-primary-mark-cache). |
| CH-07 | Decompressed column block cache with admission control | Reduce repeated codec CPU | Retained heap and stale blocks | [x] Implemented as the opt-in `TypedTableColumnarCacheOptions.DecompressedBlockCache`; see [TYPED_TABLE_DECOMPRESSED_BLOCK_CACHE.md](TYPED_TABLE_DECOMPRESSED_BLOCK_CACHE.md) and [BENCHMARK.md](BENCHMARK.md#ch-007-decompressed-column-block-cache-with-admission). |
| CH-08 | Remote-part file cache with pinning and eviction priorities | Make object-store reads predictable | Local disk pressure | [x] Implemented as a bounded immutable cache; see [REMOTE_PART_CACHE.md](REMOTE_PART_CACHE.md) and [BENCHMARK.md](BENCHMARK.md#ch-008-remote-part-cache). |
| CH-09 | Asynchronous insert buffer with bounded flush batches | Amortize many tiny inserts | Visibility and crash recovery semantics | [x] Implemented as the opt-in `hat/hatCache.AsyncInsertBuffer`; it uses existing scalar journal records and group commit, with recovery semantics documented in [CH009_ASYNC_INSERT_BUFFER.md](CH009_ASYNC_INSERT_BUFFER.md). |
| CH-10 | Insert deduplication tokens for retried client batches | Make retries idempotent | Token retention and namespace rules | [x] Implemented by the opt-in journal-backed `ExecuteSQLMutationIdempotent`; see [CH057_SQL_MUTATION_IDEMPOTENCY.md](CH057_SQL_MUTATION_IDEMPOTENCY.md) and [BENCHMARK.md#ch-057-sql-mutation-idempotency](BENCHMARK.md#ch-057-sql-mutation-idempotency). |
| CH-11 | Client-facing insert quorum separate from replica write quorum | Expose durability acknowledgement explicitly | Added latency and failure modes | [ ] |
| CH-12 | Delete bitmap compaction scheduler | Compact logical-delete bitmap integrated with existing threshold compaction so scans skip tombstoned rows efficiently | Background CPU and rewrite spikes | [x] |
| CH-13 | Mutation throttling with maintenance windows | Protect foreground queries | Longer mutation completion time | [x] |
| CH-14 | Mutation dependency graph with resumable progress | Make overlapping mutations safe to operate | Persistent metadata | [ ] |
| CH-15 | TTL movement executor across hot, warm, and cold volumes | Automate tier placement | I/O movement and recovery complexity | [ ] |
| CH-16 | Tier-aware remote prefetch and read fallback | Hide object-store latency for sequential scans | Extra bandwidth and cache pollution | [x] |
| CH-17 | Workload-driven projection creation advisor | Find high-value projections without guesswork | Bad recommendations can waste disk | [ ] |
| CH-18 | Projection refresh lag and failure state | Make stale materialized data visible | More status bookkeeping | [x] Implemented as a bounded process-local `IncrementalProjectionRunner.Status` snapshot; see [CH018_PROJECTION_REFRESH_STATUS.md](CH018_PROJECTION_REFRESH_STATUS.md) and [BENCHMARK.md](BENCHMARK.md#ch-018-projection-refresh-lag-and-failure-state). |
| CH-19 | Projection storage budget and admission policy | Prevent projections from consuming all disk | Rejected refreshes need operator handling | [x] Implemented by `MaterializedViewsOptions` and atomic create/refresh admission; see [CH019_MATERIALIZED_VIEW_BUDGET.md](CH019_MATERIALIZED_VIEW_BUDGET.md) and [BENCHMARK.md](BENCHMARK.md#ch-019-materialized-view-storage-admission). |
| CH-20 | Parallel-replica read coordinator | Spread large reads over replicas | Coordination and duplicate work | [ ] |
| CH-21 | Read-in-order planning for early LIMIT completion | Stop ordered scans sooner | Planner restrictions | [x] Implemented by propagating the ordered-stream stop signal through materialized `ExecuteSQLQuery` when no explicit `MaxRows` source budget requires a full scan; see [CH021_READ_IN_ORDER.md](CH021_READ_IN_ORDER.md) and [BENCHMARK.md](BENCHMARK.md#ch-021-read-in-order-early-limit-completion). |
| CH-22 | Explain output for index decisions, marks, and skipped ranges | Make pruning measurable | More explain plumbing | [x] Implemented as typed `ExplainStep.Pruning` data and tabular EXPLAIN ANALYZE fields; see [CH022_EXPLAIN_PRUNING.md](CH022_EXPLAIN_PRUNING.md) and [BENCHMARK.md#ch-022-structured-explain-pruning-telemetry](BENCHMARK.md#ch-022-structured-explain-pruning-telemetry). |
| CH-23 | Automatic primary-key prefix tuning from workload history | Improve common range scans | Layout changes are expensive | [x] Implemented as read-only `SQLIndexAdvisor.PrimaryPrefixRecommendations`; equality-before-range prefixes are bounded and persisted, while live layout mutation remains explicitly off. See [SQL_INDEX_ADVISOR.md](SQL_INDEX_ADVISOR.md). |
| CH-24 | Skip-index false-positive and usefulness telemetry | Retire indexes that do not pay back | Metrics overhead | [x] Implemented as explain-only residual pruning counters; see [CH024_SKIP_INDEX_USEFULNESS.md](CH024_SKIP_INDEX_USEFULNESS.md) and [BENCHMARK.md#ch-24-skip-index-usefulness-telemetry](BENCHMARK.md#ch-24-skip-index-usefulness-telemetry). |
| CH-25 | Full-text inverted postings index | Accelerate token and phrase search | Index size and update cost | [x] |
| CH-26 | Phrase and proximity search over text postings | Improve search precision | More postings metadata | [x] Implemented by `CONTAINS_PHRASE` and `CONTAINS_PROXIMITY` over the existing opt-in text index; see [SQL_TEXT_PHRASE.md](SQL_TEXT_PHRASE.md) and [BENCHMARK.md#ch-026-sql-phrase-and-proximity-search](BENCHMARK.md#ch-026-sql-phrase-and-proximity-search). |
| CH-27 | External dictionary cache with bounded refresh | Adopted as the public bounded `hat/hatDictionary` cache with batch refresh, TTL, stale-on-error opt-in, and entry/byte limits. | Source failures and stale values | [x] |
| CH-28 | Dictionary version and fallback semantics | Adopted optional source versions, version-pinned refresh/lookup, and explicit miss/error fallback with result provenance. | Version coordination | [x] |
| CH-29 | Dictionary-backed join execution | Adopted `EXTERNAL` point-lookup `INNER`/`LEFT` joins with `hatDictionary.NewSQLDictionaryLookupResolver`; optional expected versions prevent mixed dictionary snapshots. | Refresh consistency | [x] |
| CH-30 | Map key/value subcolumn pruning | Read only the needed nested vector | Planner and null semantics | [x] Implemented for one-level JSON object paths through `ColumnarMapColumn` and the optional `ColumnarMapSubcolumnSourceResolver`; unsupported paths retain the established fallback. See [CH030_MAP_SUBCOLUMNS.md](CH030_MAP_SUBCOLUMNS.md) and [BENCHMARK.md](BENCHMARK.md#ch-030-map-keyvalue-subcolumn-pruning). |
| CH-31 | Automatic typed subcolumn materialization for JSON paths | Turn hot JSON paths into compact columns | Schema churn and disk use | [x] Adopted as an opt-in bounded `hatSql.JSONSubcolumnAutoMaterializer`; source generations prevent stale reuse and cold/unsupported paths retain the normal fallback. See [CH031_AUTOMATIC_TYPED_JSON_SUBCOLUMNS.md](CH031_AUTOMATIC_TYPED_JSON_SUBCOLUMNS.md) and [BENCHMARK.md](BENCHMARK.md#ch-031-automatic-typed-json-subcolumn-promotion). |
| CH-32 | Decimal128 and Decimal256 physical codecs | Exact high-precision analytics | Arithmetic CPU and overflow rules | [x] Implemented by `hatSql.SQLDecimal128`, `hatSql.SQLDecimal256`, fixed-width RowBinary codecs, explicit stream metadata, schema validation, and exact stats/pruning support; see [SQL_DECIMAL_TYPES.md](SQL_DECIMAL_TYPES.md) and [BENCHMARK.md](BENCHMARK.md#ch-032-fixed-width-sql-decimal-rowbinary). |
| CH-33 | UUID typed encoding and scalar functions | Compact stable identifiers | Type compatibility | [ ] Evaluated and reverted: fixed 16-byte backing retained 3.25x less physical payload, but the measured physical scan was 1.99x slower and public reads were 2.93x slower with an extra 48 B/op allocation. See [BENCHMARK.md](BENCHMARK.md#ch-033-uuid-typed-table-storage-evaluation). |
| CH-34 | IPv4 and IPv6 typed encoding and functions | Compact network analytics | Parsing and ordering semantics | [x] |
| CH-35 | Enum physical encoding and schema validation | Compact categorical values | Schema evolution constraints | [x] Implemented by `hatSql.SQLRowBinaryEnum8`, `hatSql.SQLRowBinaryEnum16`, schema-aware enum labels, strict code validation, and generated `hatSchema` enum model types; see [SQL_ENUM_TYPES.md](SQL_ENUM_TYPES.md) and [BENCHMARK.md](BENCHMARK.md#ch-035-compact-sql-enum-rowbinary-values). |
| CH-36 | SQL aggregate combinators such as If, OrNull, State, and Merge | Reuse partial aggregate states in SQL | Parser and type-system growth | [x] Partially adopted: existing `*_IF` forms plus opt-in `COUNT_STATE`/`SUM_STATE`/`AVG_STATE`/`MIN_STATE`/`MAX_STATE` and matching `*_MERGE` functions; `OrNull` remains open. See [AGGREGATE_COMBINATORS.md](AGGREGATE_COMBINATORS.md) and [BENCHMARK.md](BENCHMARK.md#ch-036-sql-aggregate-state-and-merge). |
| CH-37 | argMin and argMax aggregate states | Select values associated with extrema | Tie and NULL semantics | [x] Implemented as `ARGMAX_STATE`/`ARGMIN_STATE` plus matching `ARGMAX_MERGE`/`ARGMIN_MERGE` with exact scalar state encoding, stored collation, NULL skipping, stable first-winner ties, and strict malformed-state validation; see [AGGREGATE_COMBINATORS.md](AGGREGATE_COMBINATORS.md) and [BENCHMARK.md](BENCHMARK.md#ch-037-argmin-argmax-aggregate-state). |
| CH-38 | Bitmap aggregate and set-operation functions | Fast membership and cardinality analytics | Bitmap memory | [x] |
| CH-39 | Automatic selection among exact and approximate uniq states | Match error targets to workload | Result stability and configuration | [x] |
| CH-40 | TDigest and GK quantile state variants | Better tail quantile accuracy choices | State size and merge cost | [x] Implemented by opt-in `APPROX_TDIGEST_PERCENTILE` and exported `hatDataStructure.TDigest`; see [SQL_TDIGEST_PERCENTILE.md](SQL_TDIGEST_PERCENTILE.md) and [BENCHMARK.md](BENCHMARK.md#ch-040-t-digest-percentile). |
| CH-41 | Bounded groupArray aggregate state | Retain representative grouped values safely | Truncation semantics | [x] |
| CH-42 | Window frame exclusion and group-frame semantics | Cover more analytical SQL | Complex incremental maintenance | [x] Implemented for aggregate and arg-extreme windows; see [SQL_WINDOW_FRAME_EXCLUSION.md](SQL_WINDOW_FRAME_EXCLUSION.md) and [BENCHMARK.md](BENCHMARK.md#ch-042-window-frame-exclusion). |
| CH-43 | ASOF temporal join | Match each row to the latest earlier dimension value | Ordering and timestamp edge cases | [ ] |
| CH-44 | Interval join maintenance | Join rows whose validity intervals overlap | Retained interval indexes | [ ] |
| CH-45 | Distributed GLOBAL IN and GLOBAL JOIN broadcast planning | Avoid repeated remote subqueries | Network amplification | [ ] |
| CH-46 | Kafka table source with durable offset checkpoints | Ingest streams without an external adapter | Exactly-once and connector lifecycle | [ ] |
| CH-47 | S3 and URL table functions with ranged reads | Query external files selectively | Remote failures and credentials | [ ] |
| CH-48 | External-format schema inference with controlled type promotion | Reduce ingestion setup | Surprising type changes | [x] Bounded `hatSql` inference for rows, JSON, NDJSON, and registered external tables; conservative JSON fallback and opt-in numeric promotion. See [CH048_EXTERNAL_SCHEMA_INFERENCE.md](CH048_EXTERNAL_SCHEMA_INFERENCE.md) and [BENCHMARK.md](BENCHMARK.md#ch-048-external-schema-inference) |
| CH-49 | Encrypted backups with key rotation metadata | Protect durable snapshots | Key management and restore tooling | [x] Opt-in AES-256-GCM object-store payload/manifest encryption with keyring rotation and authenticated restore; local tar/Pebble bundles remain a separate follow-up. See [BACKUP_ENCRYPTION.md](BACKUP_ENCRYPTION.md) |
| CH-50 | SQL RowBinary streaming import and export | Minimize large SQL transfer overhead | Format compatibility and limits | [x] Additive HTTP export via `Accept: application/x-hatrie-rowbinary` plus bounded embedded/HTTP import via `ExecuteSQLRowBinaryInsert`; see [CH050_SQL_ROW_BINARY_STREAM.md](CH050_SQL_ROW_BINARY_STREAM.md) |
| CH-51 | LowCardinality dictionary encoding for repeated strings | Reduce column memory, GROUP BY work, and transfer bytes | High-cardinality build cost and dictionary limits | [x] Implemented as opt-in `hatDataStructure.LowCardinalityStringColumn` with packed codes, dense counts, bounded `HLC1` binary, and a conservative default distinct limit; see [CH051_LOW_CARDINALITY.md](CH051_LOW_CARDINALITY.md) and [BENCHMARK.md](BENCHMARK.md#ch-051-low-cardinality-string-columns). |

### Delivered follow-up

- **CH-030a prepared SQL/JSON path programs**: literal and bound-parameter
  paths are compiled once per prepared expression; dynamic paths retain
  runtime parsing. This is an incremental planner step toward CH-30 and
  CH-31, not full nested subcolumn storage. See
  [CH030_PREPARED_JSON_PATHS.md](CH030_PREPARED_JSON_PATHS.md) and
  [BENCHMARK.md#ch-030a-prepared-sqljson-path-programs](BENCHMARK.md#ch-030a-prepared-sqljson-path-programs).
- **CH-051 prepared SQL regex programs**: literal patterns are compiled once
  for `REGEXP_LIKE`, `REGEXP_EXTRACT`, and row-mode `REGEXP` predicates;
  dynamic patterns retain runtime compilation. See
  [CH051_PREPARED_REGEX_PROGRAMS.md](CH051_PREPARED_REGEX_PROGRAMS.md) and
  [BENCHMARK.md#ch-051-prepared-sql-regex-programs](BENCHMARK.md#ch-051-prepared-sql-regex-programs).

## Materialize (50 candidates)

| ID | Candidate gap | Intended value | Main cost or risk | State |
| --- | --- | --- | --- | --- |
| MZ-01 | Durable persisted collections with blob and consensus handles | Recover arrangements without full recompute | Storage protocol and GC complexity | [ ] |
| MZ-02 | Public since and upper read holds | Pin historical reads safely | Leaked holds block compaction | [x] `TypedTable.AcquireChangeReadHold` pins multi-call changefeed snapshots and blocks unsafe compaction; see [MZ002_TYPED_TABLE_READ_HOLDS.md](MZ002_TYPED_TABLE_READ_HOLDS.md) and [BENCHMARK.md#mz-002-typedtable-change-read-holds](BENCHMARK.md#mz-002-typedtable-change-read-holds). |
| MZ-03 | Frontier-aware logical compaction scheduler | Bound retained differential history | Scheduler and prioritization cost | [x] Partially adopted as the opt-in `hatPipeline.FrontierCompactionScheduler` and `FrontierRetentionRegistry.WaitUntilSafe`; it gates compaction before worker admission without changing defaults. See [MZ003_FRONTIER_COMPACTION_SCHEDULER.md](MZ003_FRONTIER_COMPACTION_SCHEDULER.md) and [BENCHMARK.md#mz-003-frontier-aware-compaction-admission](BENCHMARK.md#mz-003-frontier-aware-compaction-admission). |
| MZ-04 | Compaction debt and blocked-frontier metrics | Explain storage growth | More telemetry state | [x] Implemented as allocation-free fields on `hatPipeline.FrontierRetentionSnapshot`; see [MZ04_FRONTIER_COMPACTION_METRICS.md](MZ04_FRONTIER_COMPACTION_METRICS.md) and [BENCHMARK.md#mz-04-frontier-compaction-debt-metrics](BENCHMARK.md#mz-04-frontier-compaction-debt-metrics). |
| MZ-05 | Immutable sealed batch/run format for persisted updates | Stream compaction efficiently | New on-disk format | [ ] |
| MZ-06 | Blob garbage collection at a verified safe frontier | Reclaim durable history safely | Recovery coordination | [x] Partially adopted for content-addressed incremental backups: `hatBackup.ObjectStoreTarget` now plans and applies exact orphan-object deletion from a validated retention plan, with missing-keep refusal and unknown-key preservation. Distributed frontier locking and atomic delete remain open. See [MZ006_OBJECT_STORE_GARBAGE_COLLECTION.md](MZ006_OBJECT_STORE_GARBAGE_COLLECTION.md) and [BENCHMARK.md#mz-006-verified-object-store-garbage-collection](BENCHMARK.md#mz-006-verified-object-store-garbage-collection). |
| MZ-07 | Frontier-aware source backpressure | Avoid unbounded lagging input | Source throughput reduction | [x] Adopted as the opt-in `hatPipeline.FrontierBackpressure` gate; see [MZ007_FRONTIER_SOURCE_BACKPRESSURE.md](MZ007_FRONTIER_SOURCE_BACKPRESSURE.md) and [BENCHMARK.md#mz-07-frontier-aware-source-backpressure](BENCHMARK.md#mz-07-frontier-aware-source-backpressure). |
| MZ-08 | Compact antichain representation for multi-dimensional frontiers | Reduce timestamp metadata | Harder comparison code | [x] Implemented by the bounded flat `hatPipeline.FrontierAntichain`; see [MZ008_FRONTIER_ANTICHAIN.md](MZ008_FRONTIER_ANTICHAIN.md) and [BENCHMARK.md#mz-08-compact-frontier-antichain](BENCHMARK.md#mz-08-compact-frontier-antichain). |
| MZ-09 | Timestamp-domain leases for independent source clocks | Prevent timestamp collisions | Lease expiry and coordination | [ ] |
| MZ-10 | Cross-source snapshot cutover coordinator | Start a consistent multi-source view | Connector synchronization | [ ] |
| MZ-11 | Kafka partition offset frontiers | Expose exact source completeness | Offset and timestamp mapping | [ ] |
| MZ-12 | Kafka rebalance fencing for stale consumers | Prevent duplicate ownership | Consumer-group coordination | [ ] |
| MZ-13 | Debezium envelope normalization into signed updates | Simplify CDC ingestion | Schema and tombstone handling | [ ] |
| MZ-14 | Avro schema-registry source integration | Evolve event schemas safely | Registry availability | [ ] |
| MZ-15 | Protobuf schema-registry source integration | Compact typed CDC transfer | Field compatibility rules | [ ] |
| MZ-16 | Exactly-once source offset checkpoints coupled to updates | Make restart replay bounded | Durable transaction protocol | [ ] |
| MZ-17 | Source transaction grouping and commit markers | Preserve upstream transaction boundaries | Delayed visibility | [ ] |
| MZ-18 | Source freshness constraints with query rejection or wait | Make staleness explicit | Availability tradeoff | [x] [MZ018_SOURCE_FRONTIER_WAIT.md](MZ018_SOURCE_FRONTIER_WAIT.md) |
| MZ-19 | Durable connector pause and resume state | Operate connectors without losing progress | State-machine complexity | [ ] |
| MZ-20 | Two-phase sink progress checkpoints | Prevent partial sink publication | Sink protocol requirements | [ ] |
| MZ-21 | Sink idempotency tokens derived from frontier and batch | Make retries safe | Token retention | [ ] |
| MZ-22 | Adaptive sink batching and flush deadlines | Improve throughput without large latency spikes | Tuning complexity | [-] Evaluated and rejected: the same 100-record replay measured `1.032x` CPU and `1.074x` transient heap versus fixed batching despite four fewer allocations; see [BENCHMARK.md#mz-022-adaptive-sink-batching-evaluation](BENCHMARK.md#mz-022-adaptive-sink-batching-evaluation). |
| MZ-23 | Sink delivery audit trail with source frontier | Diagnose missing or delayed output | Event volume | [ ] |
| MZ-24 | Resumable TAIL cursor tokens | Reconnect subscriptions without full replay | Cursor retention and invalidation | [x] |
| MZ-25 | TAIL progress and heartbeat records | Distinguish idle from stalled streams | Protocol surface | [x] Implemented by `hatSql.QuerySubscriptions.Heartbeat` plus opt-in `EmitProgress`; see [MZ025_TAIL_HEARTBEAT.md](MZ025_TAIL_HEARTBEAT.md) and [BENCHMARK.md#mz-25-tail-progress-and-heartbeats](BENCHMARK.md#mz-25-tail-progress-and-heartbeats). |
| MZ-26 | Subscription snapshot export at an exact frontier | Bootstrap downstream consumers deterministically | Snapshot cost | [x] Implemented by `QuerySubscriptions.ExportSnapshotsAt`; see [MZ026_SUBSCRIPTION_SNAPSHOT_EXPORT.md](MZ026_SUBSCRIPTION_SNAPSHOT_EXPORT.md) and [BENCHMARK.md#mz-26-exact-frontier-subscription-snapshot-export](BENCHMARK.md#mz-26-exact-frontier-subscription-snapshot-export). |
| MZ-27 | Read-hold lifecycle and compaction pin diagnostics | Find clients blocking compaction | Handle tracking | [x] Implemented by `hatPipeline.FrontierRetentionRegistry.ActiveLeases`; see [MZ027_READ_HOLD_DIAGNOSTICS.md](MZ027_READ_HOLD_DIAGNOSTICS.md) and [BENCHMARK.md#mz-27-read-hold-lifecycle-diagnostics](BENCHMARK.md#mz-27-read-hold-lifecycle-diagnostics). |
| MZ-28 | Temporal join arrangements keyed by valid-time intervals | Maintain time-aware dimensions incrementally | Interval update complexity | [ ] |
| MZ-29 | Differential interval-join maintenance | Avoid rescanning overlapping ranges | Index memory | [x] Implemented as the imported `hatSql.IncrementalIntervalJoin` API; see [MZ029_INCREMENTAL_INTERVAL_JOIN.md](MZ029_INCREMENTAL_INTERVAL_JOIN.md). |
| MZ-30 | Lookup-join cache invalidation by source frontier | Keep lookup results consistent | Cache coordination | [ ] |
| MZ-31 | Incremental Top-K change stream with rank movement diffs | Avoid complete result replacement | Ranking state and churn | [ ] |
| MZ-32 | Arrangement cost model and reuse scoring | Share indexes only when they pay back | Planner estimation errors | [ ] |
| MZ-33 | Automatic index recommendation from observed dataflows | Reduce manual tuning | Background analysis cost | [ ] |
| MZ-34 | Online index build scheduler with admission limits | Build indexes without starving queries | Build latency | [ ] |
| MZ-35 | Arrangement shard locality hints | Keep hot keys near their consumers | Skew and rebalancing | [ ] |
| MZ-36 | Dataflow operator placement constraints by failure domain | Improve locality and resilience | Placement solver complexity | [ ] |
| MZ-37 | Worker-local exchange batching | Reduce per-record coordination | Batch latency | [x] |
| MZ-38 | Dynamic dataflow worker scaling | Match compute to changing load | State movement and rebalance | [ ] |
| MZ-39 | Operator fuel or yield budgets | Bound a single operator monopolization | More scheduler checks | [x] |
| MZ-40 | Recursive convergence diagnostics and iteration bounds | Make recursive dataflows operable | Additional state and errors | [x] |
| MZ-41 | Recursive negative differential propagation | Support deletions in recursive results | Non-monotone fixpoint complexity | [ ] |
| MZ-42 | Dataflow dependency graph introspection | Explain rebuild and invalidation impact | Graph retention | [ ] |
| MZ-43 | Per-operator frontier lag metrics | Locate the actual source of staleness | Metrics cardinality | [x] |
| MZ-44 | Session snapshot consistency tokens | Tie multiple queries to one logical view | Token lifetime | [x] Implemented by the opt-in HMAC-backed `hatSql.SQLSnapshotTokenCodec`; it maps to the existing `AsOfFrontier` provider path and defaults off. See [MZ044_SNAPSHOT_TOKENS.md](MZ044_SNAPSHOT_TOKENS.md) and [BENCHMARK.md#mz-044-session-snapshot-tokens](BENCHMARK.md#mz-044-session-snapshot-tokens). |
| MZ-45 | Transactional source-to-sink boundary | Commit a source batch and sink effect together | Distributed commit complexity | [ ] |
| MZ-46 | Schema migration barrier across dependent dataflows | Prevent mixed-schema results | Planned downtime or buffering | [ ] |
| MZ-47 | Timeline recovery and frontier reconciliation | Repair partially persisted progress | Recovery duration | [ ] |
| MZ-48 | Persisted catalog manifest migrations | Upgrade metadata atomically | Migration compatibility | [ ] |
| MZ-49 | Connector schema-drift quarantine stream | Keep bad records from stopping a source | Quarantine storage and policy | [x] Bounded callback quarantine for decoded rows and `UseNumber` JSONEachRow input with deterministic drift diagnostics and explicit limits; see [MZ049_SCHEMA_DRIFT_QUARANTINE.md](MZ049_SCHEMA_DRIFT_QUARANTINE.md) and [BENCHMARK.md](BENCHMARK.md#mz-049-schema-drift-quarantine) |
| MZ-50 | Query-plan snapshots annotated with timestamp and frontier requirements | Make temporal readiness explainable | Explain output size | [x] Implemented as opt-in `SQLPlanSnapshot` metadata on materialized SQL results; see [MZ050_PLAN_SNAPSHOTS.md](MZ050_PLAN_SNAPSHOTS.md) and [BENCHMARK.md#mz-050-sql-plan-snapshots](BENCHMARK.md#mz-050-sql-plan-snapshots). |

## Tarantool (50 candidates)

| ID | Candidate gap | Intended value | Main cost or risk | State |
| --- | --- | --- | --- | --- |
| TR-01 | Raft-backed failover with leader leases | Prevent split-brain promotions | Consensus latency and operational complexity | [ ] |
| TR-02 | Synchronous replication commit barriers | Acknowledge only after durable replicas | Write latency and quorum failures | [ ] |
| TR-03 | Replica-promotion catch-up barrier | Avoid promoting a stale node | Longer failover | [ ] |
| TR-04 | Replication filtering by space and key range | Reduce relay bandwidth | Divergent replica contents | [x] Implemented as opt-in literal `HTTPReplicatorOptions.ReplicationKeyPrefixes`; see [TR004_REPLICATION_KEY_FILTER.md](TR004_REPLICATION_KEY_FILTER.md) and [BENCHMARK.md#tr-04-replication-key-prefix-filter](BENCHMARK.md#tr-04-replication-key-prefix-filter). |
| TR-05 | Parallel relay and applier queues with ordering fences | Increase replication throughput | Ordering and backpressure bugs | [ ] |
| TR-06 | Conflict-resolution hooks for multi-master updates | Make application conflict policy explicit | Non-deterministic user code | [ ] |
| TR-07 | Adaptive WAL group-commit and fsync policy | Improve write throughput safely | Durability window changes | [ ] |
| TR-08 | WAL encryption and key-rotation metadata | Protect journals at rest | Key recovery and CPU cost | [ ] |
| TR-09 | Incremental snapshot chains | Reduce backup write volume | Chain recovery complexity | [ ] |
| TR-10 | Snapshot manifests with atomic restore publication | Make restores verifiable and resumable | Manifest compatibility | [ ] |
| TR-11 | Hot-backup consistent file-set coordination | Copy live storage without torn state | Snapshot coordination | [ ] |
| TR-12 | Memtx and Vinyl-style hot and cold storage migration | Match storage to access temperature | Movement and consistency | [ ] |
| TR-13 | Vinyl-style compaction debt scheduler | Smooth LSM rewrite pressure | Background I/O | [ ] |
| TR-14 | LSM-run Bloom-filter sidecars | Skip cold runs on point lookups | Filter memory and false positives | [x] Already implemented as the opt-in native LevelDB/Pebble Bloom policy documented as `TT-017`; the default remains off because the measured warm workload increased storage without a general CPU win. |
| TR-15 | Bloom false-positive and run-read telemetry | Tune filters from evidence | Metrics overhead | [x] Exposes Pebble filter hits/misses and LSM read amplification through portable persistent-store inspection; LevelDB remains zero-valued and defaults/storage behavior are unchanged. See [TR015_PERSISTENT_FILTER_TELEMETRY.md](TR015_PERSISTENT_FILTER_TELEMETRY.md) and [BENCHMARK.md#tr-015-persistent-filter-and-read-amplification-telemetry](BENCHMARK.md#tr-015-persistent-filter-and-read-amplification-telemetry). |
| TR-16 | Page-cache admission and pinning policy | Keep hot pages resident | Memory pressure | [x] Implemented in [TR016_STORAGE_PINNING.md](TR016_STORAGE_PINNING.md) |
| TR-17 | Tuple arena or slab allocation | Reduce per-tuple allocator overhead | Fragmentation and lifetime rules | [x] Implemented as the safe single-source execution-row fast path and projection-map pre-sizing; multi-source join tuples retain their required merge maps. See [TR017_SINGLE_SOURCE_ROW_FASTPATH.md](TR017_SINGLE_SOURCE_ROW_FASTPATH.md). |
| TR-18 | Zero-copy tuple field slices | Avoid copying large values on reads | Borrowed-memory lifetime hazards | [x] Implemented in [TR018_ZERO_COPY_ROW_BINARY.md](TR018_ZERO_COPY_ROW_BINARY.md) |
| TR-19 | Tuple field-offset cache | Accelerate repeated field access | Schema invalidation | [x] Implemented as an opt-in schema-aware `ColumnarBatch.PrepareFieldOffsets` cache with mutation invalidation; see [TR019_TUPLE_FIELD_OFFSETS.md](TR019_TUPLE_FIELD_OFFSETS.md) and [BENCHMARK.md](BENCHMARK.md#tr-019-tuple-field-offset-cache). |
| TR-19a | Plain columnar value dispatch fast path | Avoid probing unused physical layouts | Mixed-layout branch | [x] Implemented by the C168 plain-only `ColumnarBatch.Value` path; see [TR019_COLUMNAR_VALUE_FASTPATH.md](TR019_COLUMNAR_VALUE_FASTPATH.md). |
| TR-20 | Tuple schema-version validation on every boundary | Reject incompatible records early | Version metadata | [ ] |
| TR-21 | Online secondary-index build | Add indexes without blocking writes | Build/replay resource usage | [ ] |
| TR-22 | Online secondary-index rebuild and verification | Repair indexes safely | Double storage and I/O | [ ] |
| TR-23 | Functional indexes over deterministic expressions | Accelerate computed predicates | Expression compatibility | [ ] |
| TR-24 | Covering indexes with projected payload fields | Avoid primary tuple fetches | Larger indexes and staleness | [ ] |
| TR-25 | Multi-column prefix range planner | Use composite indexes efficiently | Planner complexity | [ ] |
| TR-26 | Bitmap indexes for low-cardinality fields | Fast set intersections | Update and memory cost | [x] Implemented by the typed `hatDataStructure.BitmapIndex[K]`; see [TR026_BITMAP_INDEX.md](TR026_BITMAP_INDEX.md) and [BENCHMARK.md#tr-026-typed-bitmap-index](BENCHMARK.md#tr-026-typed-bitmap-index). |
| TR-27 | Spatial R-tree index for bounded geometry queries | Avoid full spatial scans | Complex update semantics | [ ] |
| TR-28 | Cursor pagination resume tokens | Resume scans without offset work | Token signing and invalidation | [ ] |
| TR-29 | Reverse index iterators with stable bounds | Efficient newest-first reads | Mutation and cursor semantics | [x] Implemented by `OrderedIndex.Last`, reverse seeks, and `LastSnapshotCursor`; see [TR029_REVERSE_ITERATORS.md](TR029_REVERSE_ITERATORS.md) and [BENCHMARK.md](BENCHMARK.md#tr-029-reverse-ordered-index-iterators). |
| TR-30 | Index selectivity and distribution statistics | Improve index choice | Statistics maintenance | [ ] |
| TR-31 | Automatic index choice with explainable fallback | Reduce caller tuning | Planner regressions | [ ] |
| TR-32 | Conditional compare-and-swap update primitive | Avoid read-modify-write races | Predicate semantics | [x] |
| TR-33 | Returning old and new tuple values from mutations | Build changefeeds without rereads | Copy cost and API shape | [x] Implemented as `SQLMutationResult.BeforeRows` for affected SQL mutations with `RETURNING`; existing `Rows` remains the post-state (or deleted row). See [TR033_SQL_RETURNING_BEFORE_ROWS.md](TR033_SQL_RETURNING_BEFORE_ROWS.md) and [BENCHMARK.md#tr-033-sql-returning-before-rows](BENCHMARK.md#tr-033-sql-returning-before-rows). |
| TR-34 | Nested transaction savepoints | Roll back part of a complex operation | Undo-log complexity | [ ] |
| TR-35 | MVCC snapshot read views | Stable reads during concurrent writes | Version retention | [ ] |
| TR-36 | Explicit read-only transaction mode | Protect analytical clients from writes | API and enforcement work | [x] Implemented by `SQLTransactionOptions.ReadOnly`; see [TR036_READ_ONLY_TRANSACTIONS.md](TR036_READ_ONLY_TRANSACTIONS.md) and [BENCHMARK.md](BENCHMARK.md#tr-036-read-only-sql-transactions). |
| TR-37 | Deadlock detection with a wait-for graph | Fail blocked transactions deterministically | Graph overhead | [ ] |
| TR-38 | Transaction and statement timeouts | Bound stuck work | Partial rollback behavior | [x] Transaction-wide timeout is implemented through `SQLTransactionOptions.Timeout`; existing statement timeout remains `SQLQueryOptions.Timeout`. See [TR038_TRANSACTION_TIMEOUT.md](TR038_TRANSACTION_TIMEOUT.md) and [BENCHMARK.md](BENCHMARK.md#tr-038-sql-transaction-timeouts). |
| TR-39 | Fiber-local allocation pools and context | Lower scheduler-path allocations | Lifetime leaks | [ ] |
| TR-40 | Cancellation propagation into fibers | Stop abandoned requests quickly | Cooperative cancellation gaps | [ ] |
| TR-41 | Net.box-style request multiplexing | Reuse one connection for concurrent calls | Ordering and head-of-line blocking | [ ] |
| TR-42 | Net.box streaming cursors with backpressure | Process large reads incrementally | Cursor ownership | [ ] |
| TR-43 | IProto prepared request IDs and response schemas | Reduce repeated parsing | Schema negotiation | [ ] |
| TR-43a | Prepared compact peer request call path | Reuse command encoding and reduce per-call allocation | Opt-in API and bounded scratch buffer | [x] Implemented by `CompactPeerSession.CallTemplate`; compressed sessions retain the existing writer path and large request buffers are released. See [TR043A_COMPACT_PEER_PREPARED_CALL.md](TR043A_COMPACT_PEER_PREPARED_CALL.md) and [BENCHMARK.md#tr-043a-prepared-compact-peer-calls](BENCHMARK.md#tr-043a-prepared-compact-peer-calls). |
| TR-43b | Reusable compact peer frame writer buffer | Remove repeated plain request/response frame allocations | Up to 64 KiB scratch retention per active session | [x] Implemented by `CompactProtocol.MarshalInto` and bounded plain-session writer reuse; compressed sessions keep the existing writer path. See [TR043B_COMPACT_PEER_WRITE_BUFFER.md](TR043B_COMPACT_PEER_WRITE_BUFFER.md) and [BENCHMARK.md#tr-043b-reusable-compact-peer-frame-buffers](BENCHMARK.md#tr-043b-reusable-compact-peer-frame-buffers). |
| TR-44 | IProto compression negotiation per request class | Save bandwidth selectively | CPU and protocol fallback | [x] Compact peer payloads support threshold-based gzip with an explicit frame flag and bounded inflation; disabled by default because the measured CPU tradeoff is significant. Listener-managed sessions now negotiate the compression capability; direct sessions remain opt-in. See [BENCHMARK.md](BENCHMARK.md#tr-044-selective-compact-peer-payload-compression). |
| TR-45 | Connection circuit breaker and health scoring | Stop sending to failing peers | Recovery tuning | [ ] |
| TR-46 | Schema and DDL discovery protocol | Keep clients compatible during changes | Version drift | [ ] |
| TR-47 | Role and object-grant authorization model | Narrow access beyond bearer authentication | Policy administration | [x] Implemented by optional `hatAuth.Rule.Objects` and `Policy.AuthorizeObject`; see [TR047_OBJECT_GRANTS.md](TR047_OBJECT_GRANTS.md) and [BENCHMARK.md](BENCHMARK.md#tr-047-object-scoped-rbac-grants). |
| TR-48 | Audit-event sampling and export sinks | Operate high-volume audit safely | Dropped-event visibility | [x] Implemented by `AuditLoggerOptions.SuccessSampleRate`, lossless failures, and `AuditSink`; see [TR048_AUDIT_SAMPLING.md](TR048_AUDIT_SAMPLING.md) and [BENCHMARK.md](BENCHMARK.md#tr-048-audit-event-sampling-and-export-sinks). |
| TR-49 | Queue partition ownership and online migration | Scale queues without implicit sharding | Movement and backup semantics | [ ] |
| TR-50 | Rate-aware WAL and replication backpressure | Preserve foreground latency under bursts | Lower write throughput | [ ] |
| TR-51 | Compact peer capability negotiation for compression | Avoid sending flagged frames to incompatible peers | One fixed handshake round trip and feature-bit API | [x] Implemented by `CompactPeerFeaturePayloadCompression`; listener-managed sessions enable gzip only after negotiation, while direct sessions remain backward-compatible. See [TR051_COMPACT_PEER_CAPABILITY_NEGOTIATION.md](TR051_COMPACT_PEER_CAPABILITY_NEGOTIATION.md) and [BENCHMARK.md#tr-051-compact-peer-capability-negotiation](BENCHMARK.md#tr-051-compact-peer-capability-negotiation). |
| TR-52 | Small-vector secondary-index representation | Keep tiny ordered indexes compact and fast | Linear ID lookup below the promotion threshold | [x] Implemented by the bounded linear `OrderedIndex` representation with map promotion above 32 entries; see [TR052_ORDERED_INDEX_SMALL_VECTOR.md](TR052_ORDERED_INDEX_SMALL_VECTOR.md) and [BENCHMARK.md#tr-052-small-vector-orderedindex-representation](BENCHMARK.md#tr-052-small-vector-orderedindex-representation). |
| TR-53 | Inline-first secondary-index posting lists | Remove per-key slice allocations from sparse exact and multikey indexes | Overflow indirection and small repeated-key allocation overhead | [x] Implemented by shared `u64PostingList` storage across functional, hash, conditional, and string multikey indexes; see [TR053_COMPACT_POSTING_LIST.md](TR053_COMPACT_POSTING_LIST.md) and [BENCHMARK.md#tr-053-compact-secondary-index-posting-lists](BENCHMARK.md#tr-053-compact-secondary-index-posting-lists). |
| TR-54 | Single-task compaction scheduler direct execution | Avoid worker startup and result-array overhead for one maintenance job | Singleton callback runs on the caller goroutine | [x] Implemented as the synchronous one-task `CompactionScheduler.Run` fast path; failed tasks retain retry semantics. See [TR054_COMPACTION_SCHEDULER_SINGLE_TASK.md](TR054_COMPACTION_SCHEDULER_SINGLE_TASK.md) and [BENCHMARK.md#tr-054-single-task-compaction-scheduler-fast-path](BENCHMARK.md#tr-054-single-task-compaction-scheduler-fast-path). |
| TR-55 | Single-replica read routing fast path | Avoid channel, goroutine, and attempt-normalization overhead when only one replica is eligible | Synchronous callback on the caller for singleton reads | [x] Implemented by the direct one-node `ExecuteParallelReplicaRead` path; multi-replica hedging remains unchanged. See [TR055_SINGLE_REPLICA_READ_FASTPATH.md](TR055_SINGLE_REPLICA_READ_FASTPATH.md) and [BENCHMARK.md#tr-055-single-replica-read-routing-fast-path](BENCHMARK.md#tr-055-single-replica-read-routing-fast-path). |
| TR-56 | Single-node read quorum fast path | Avoid quorum setup and response grouping when one replica is both the only candidate and the required quorum | Synchronous callback for exactly one node and `required=1` | [x] Implemented by the direct `ExecuteReadQuorum` singleton path; invalid inputs and multi-node quorum behavior remain unchanged. See [TR056_SINGLE_NODE_READ_QUORUM_FASTPATH.md](TR056_SINGLE_NODE_READ_QUORUM_FASTPATH.md) and [BENCHMARK.md#tr-056-single-node-read-quorum-fast-path](BENCHMARK.md#tr-056-single-node-read-quorum-fast-path). |
| TR-57 | Small-vector `UpsertBatch` representation | Avoid map buckets for small Materialize-style update batches | Linear key lookup below the measured promotion threshold | [x] Implemented with a 16-key slice mode and promotion on the 17th distinct key; larger batches retain the map path. See [TR057_UPSERT_BATCH_SMALL_VECTOR.md](TR057_UPSERT_BATCH_SMALL_VECTOR.md) and [BENCHMARK.md#tr-057-small-vector-upsertbatch-representation](BENCHMARK.md#tr-057-small-vector-upsertbatch-representation). |
| TR-58 | Roaring bitmap lower-bound lookup fast path | Remove callback overhead from sorted bitmap container and array membership searches | Manual lower-bound loops must preserve sorted insertion and lookup semantics | [x] Implemented for Roaring container lookup, add, and remove; bitmap containers retain the existing bitset path. See [TR058_ROARING_BITMAP_LOOKUP_FASTPATH.md](TR058_ROARING_BITMAP_LOOKUP_FASTPATH.md) and [BENCHMARK.md#tr-058-roaring-bitmap-lookup-fast-path](BENCHMARK.md#tr-058-roaring-bitmap-lookup-fast-path). |

## CH-052 Prepared Temporal Expressions

The ClickHouse-inspired constant-expression preparation follow-up caches
literal IANA time zones in `PARSE_TIMESTAMP` and `AT TIME ZONE`, and removes a
temporary two-argument slice from the temporal evaluator. Dynamic zones keep
their runtime behavior. The measured result is 13.3x faster parsing with a
literal zone and 58.7x faster `AT TIME ZONE`, with lower bytes and allocations;
see [CH052_PREPARED_TEMPORAL_EXPRESSIONS.md](CH052_PREPARED_TEMPORAL_EXPRESSIONS.md)
and [BENCHMARK.md#ch-052-prepared-temporal-expressions](BENCHMARK.md#ch-052-prepared-temporal-expressions).

## CH-053 Prepared Literal `IN` Sets

The ClickHouse-inspired prepared-set follow-up retains literal `IN` and
`NOT IN` values after binding, eliminating one candidate-slice allocation per
row while retaining the existing linear comparison and SQL `NULL` semantics.
Dynamic lists remain unchanged. Measurements and raw samples are in
[CH053_PREPARED_LITERAL_IN.md](CH053_PREPARED_LITERAL_IN.md) and
[BENCHMARK.md#ch-053-prepared-literal-in-sets](BENCHMARK.md#ch-053-prepared-literal-in-sets).

## CH-054 Typed Prepared `IN` Search

The ClickHouse-inspired follow-up sorts large homogeneous literal sets in
place and uses typed binary search, while small, dynamic, mixed, `NULL`, and
non-binary-collation cases retain the linear comparator. It adds no second
value backing and measured up to 11.46x lower evaluation CPU; see
[CH054_TYPED_IN_SEARCH.md](CH054_TYPED_IN_SEARCH.md) and
[BENCHMARK.md#ch-054-typed-prepared-in-search](BENCHMARK.md#ch-054-typed-prepared-in-search).

## CH-055 Prepared Literal `BETWEEN` Bounds

The ClickHouse-inspired expression-preparation follow-up caches literal
`BETWEEN` and `NOT BETWEEN` bounds after binding. Dynamic bounds retain the
existing evaluator and SQL `NULL`/collation behavior. The scalar benchmark is
1.37x faster with no allocation change; see
[CH055_PREPARED_BETWEEN.md](CH055_PREPARED_BETWEEN.md) and
[BENCHMARK.md#ch-055-prepared-literal-between-bounds](BENCHMARK.md#ch-055-prepared-literal-between-bounds).

## CH-056 Prepared Literal `LIKE` Patterns

The ClickHouse-inspired expression-preparation follow-up caches the
percent-separated parts of literal string `LIKE` patterns after binding. It
also routes columnar paths through the prepared matcher and avoids raw NGram
pruning under non-binary collations. The paired benchmark is 4.72x faster with
80 B/op and 3 allocs/op reduced to 0 B/op and 0 allocs/op; see
[CH056_PREPARED_LIKE.md](CH056_PREPARED_LIKE.md) and
[BENCHMARK.md#ch-056-prepared-literal-like-patterns](BENCHMARK.md#ch-056-prepared-literal-like-patterns).

## CH-057 Journal-Backed SQL Mutation Idempotency

The ClickHouse-inspired insert-deduplication follow-up adds the explicit
`ExecuteSQLMutationIdempotent` API. It records a bounded caller token with the
exact generated command fingerprint, suppresses duplicate direct or atomic
`INSERT ... SELECT` writes, rejects conflicting token reuse, and survives
journal replay. Ordinary SQL mutations remain unchanged; `RETURNING`,
`ON CONFLICT`, `MERGE`, and automatic triggers are rejected because their
semantics are not represented by the existing public journal record. See
[CH057_SQL_MUTATION_IDEMPOTENCY.md](CH057_SQL_MUTATION_IDEMPOTENCY.md).
