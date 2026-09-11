# ClickHouse, Materialize, And Tarantool Audit

This catalog keeps 50 canonical ideas from each source product and
copies their current repository status from `INSPIRATION.md`. It is an
implementation ledger, not a claim that every idea is desirable here.

Status uses the source catalog: `[x]` adopted or verified, `[ ]` open
or intentionally deferred, and `[-]` rejected or rolled back.

## ClickHouse (50 ideas)
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
- [x] C032 CollapsingMergeTree-style sign-based row cancellation. `CollapseSQLRows` pairs unmatched opposite signs by logical key, preserves survivors in input order, and reports invalid signs; see [COLLAPSING_MERGE.md](COLLAPSING_MERGE.md).
- [x] C033 SummingMergeTree-style merge-time summation. `SumSQLRows` combines explicitly selected numeric columns with stable key order, type checks, and overflow protection; see [SUMMING_MERGE.md](SUMMING_MERGE.md).
- [x] C034 Aggregating states for reusable grouped results.
- [x] C035 Lightweight delete patch parts.
- [x] C036 Durable mutation queue with observable progress. `CommandJournal` and the LevelDB-backed `ReplicationOutboxStore` provide durable FIFO mutation recovery, dead-letter handling, bounded restore, and binary/JSON compatibility; `ReplayWithProgress` exposes concurrent replay progress and ETA. Verified with `make test-c036-durable-mutation-queue`, `make test-replay-progress`, `make test-race-replay-progress`, and `make benchmark-replay-progress`.
- [x] C037 TTL expiration for supported values and records.
- [x] C038 TTL-driven rollup or recompression. `TimeBucketRollup` supports verified bucket retention and boundary-only expiration without silently discarding partial buckets; see [TTL_ROLLUP.md](TTL_ROLLUP.md).
- [x] C039 Partition pruning for local partitions.
- [x] C040 Sampling key with deterministic SAMPLE semantics across partitions. `SampleSQLRows` hashes a caller-selected logical key with a seed, preserving selection across order and partition boundaries; see [DETERMINISTIC_SAMPLE.md](DETERMINISTIC_SAMPLE.md).
- [x] C041 Multiple disk policies with placement rules. `DiskPlacementPolicy` provides immutable weighted rules with deterministic key selection; see [DISK_PLACEMENT.md](DISK_PLACEMENT.md).
- [x] C042 Hot, warm, and cold storage tiers. `StorageTierPolicy` selects an immutable age-threshold tier and delegates path placement; see [STORAGE_TIERS.md](STORAGE_TIERS.md).
- [x] C043 Remote object-store parts with local metadata. `RemotePartReference` validates supported object URIs and root-confined metadata paths; see [REMOTE_PARTS.md](REMOTE_PARTS.md).
- [x] C044 Zero-copy replication of immutable parts. `hatMerkle.CopyImmutablePartFile` validates the declared file boundary and preserves `io.Copy` native transfer hooks; `CopyImmutablePart` supplies the verified streaming fallback. See [ZERO_COPY_PARTS.md](ZERO_COPY_PARTS.md).
- [x] C045 Part-level cache admission and eviction policy. `PartCachePolicy` provides explicit admission and deterministic LFU/LRU eviction planning; see [PART_CACHE_POLICY.md](PART_CACHE_POLICY.md).
- [x] C046 Read amplification accounting per part and column. `ReadAmplificationRegistry` and RowBinary read statistics aggregate deterministic per-part/per-column bytes and ratios; see [READ_AMPLIFICATION.md](READ_AMPLIFICATION.md).
- [x] C047 Adaptive granule sizing from observed predicate selectivity. `GranuleSizingPolicy` adapts bounded future scan granules from observed selectivity without retaining state or changing result semantics; see [GRANULE_SIZING.md](GRANULE_SIZING.md).
- [x] C048 Compact numeric encodings selected from data statistics. Adaptive RowBinary evaluates legacy, delta, and double-delta encodings from full-batch or sampled statistics and records the selected codec in the header; see [ROW_BINARY_ADAPTIVE.md](ROW_BINARY_ADAPTIVE.md).
- [x] C049 Low-cardinality dictionary encoding for typed string values.
- [x] C050 Shared JSON subcolumns for repeated paths. `JSONSubcolumnRegistry` interns normalized JSON paths into process-local `uint32` IDs with concurrent lookup and snapshots; see [JSON_SUBCOLUMNS.md](JSON_SUBCOLUMNS.md).
## Materialize (50 ideas)
- [x] M001 Incremental data-parallel dataflow for supported table paths.
- [x] M002 Generic `(data,time,diff)` multiset representation. `DifferentialMultiset[T]` stores comparable data and timestamps with signed diffs, immediate zero consolidation, and overflow-safe updates; see [DIFFERENTIAL_MULTISET.md](DIFFERENTIAL_MULTISET.md).
- [x] M003 Timely-style nested worker scopes. `hatPipeline.Scope` provides
- [x] M004 Data-parallel map, filter, project, and reduce stages where supported.
- [x] M005 Shared arrangements reused by multiple compatible queries.
- [x] M006 Arrangement keys derived from indexed predicates.
- [x] M007 Arrangement reuse across subscriptions and point reads.
- [x] M008 Arrangement compaction for old versions.
- [x] M009 Consolidation of equal data and opposite diffs. `ConsolidateDifferentialRows` combines equal key/time updates, preserves signed multiplicity, removes zero totals, and rejects overflow; see [DIFFERENTIAL_ROWS.md](DIFFERENTIAL_ROWS.md).
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
- [x] M033a Process-local lock-free Lamport timestamp allocation with monotone remote observation; see [TIMESTAMP_ORACLE.md](TIMESTAMP_ORACLE.md). Global cross-node ordering remains open.
- [x] M033b Consensus-bound global timestamp reservations with term and node-epoch fencing, idempotent per-node sequences, deterministic snapshots, and allocation-free range leases; transport and leader election remain caller-owned. See [GLOBAL_TIMESTAMP_ORACLE.md](GLOBAL_TIMESTAMP_ORACLE.md).
- [x] M034 Epoch management for restarts and leases. `hatStorage.PersistentShardLease` and `hatStorage.PersistentNodeEpoch` durably advance fencing tokens across restarts, reject concurrent owners, and expose explicit renew, release, inspect, and validation operations; see [PERSISTENT_NODE_EPOCHS.md](PERSISTENT_NODE_EPOCHS.md).
- [x] M035 Self-correcting materialized results for typed arrangements.
- [x] M036 Retractions and insertions on typed updates.
- [x] M037a Signed negative diffs in the reusable batch primitive.
- [x] M038a Duplicate multiplicity retained as signed diff weights.
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
## Tarantool (50 ideas)
- [x] T001 Compact in-memory tuple representation for hot records.
- [x] T002 LSM-backed durable storage through the Pebble path.
- [x] T003 Space-like named collections.
- [x] T004 Typed tuple or row representation.
- [x] T005 Primary-key index.
- [x] T006 Ordered TREE index behavior.
- [x] T007 HASH index behavior.
- [x] T008 RTREE spatial index. Importable uint64-ID rectangle index with deterministic overlap and point search (see SPATIAL_RTREE.md).
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
- [x] T042a Recovery replay mutation fast path - scalar durable mutations avoid constructing public command responses; unsupported commands keep the existing dispatcher (see [JOURNAL_REPLAY.md](JOURNAL_REPLAY.md)).
- [x] T043 Recovery replay progress and ETA metrics.
- [x] T044 Recovery point selection by logical sequence.
- [x] T045 Crash-consistency fault injection.
- [x] T046 Online backup cancellation with resumable manifests.
- [x] T047d Atomic public `BATCH` write quorum. Eligible all-write batches validate direct quorum before mutation, preserve local atomic commit semantics when remote acknowledgements are insufficient, and send one grouped `INTERNALBATCHV2` per target; rollback-free cluster-wide commit remains open. See [WRITE_QUORUM.md](WRITE_QUORUM.md).
- [x] T048 Replication sets and peer topology.
- [x] T049 Vector-clock exposure for every replica - replication queue results expose an immutable observational `vector_clock` containing the local sequence and all current topology members' acknowledged sequences; it does not change quorum or conflict semantics.

## Currently Open Or Deferred

These are the unchecked items from the complete source catalog;
sub-items with the same numeric ID are retained where the catalog
records a separate implementation boundary.

- [ ] C153 Metadata consensus for partition ownership.
- [ ] C154 Rolling schema changes across replicas.
- [ ] M032 Strong consistency across all independent source partitions.
- [x] M032a Opt-in SQL snapshot provider pins every source read in one query to a caller-owned immutable view; full distributed frontier coordination remains open. See [SQL_SNAPSHOT_PROVIDER.md](SQL_SNAPSHOT_PROVIDER.md).
- [ ] M033 Timestamp oracle for globally ordered writes.
- [ ] M037 Generic negative-diff support for every SQL operator.
- [ ] M037e Generic keyed differential reduction was benchmarked and rolled back because the arbitrary callback path was about 7.97x slower, 6.36x larger in transient bytes, and 5.67x more allocation-heavy than the existing specialized reducer; see [BENCHMARK.md](BENCHMARK.md#rejected-generic-keyed-differential-reduction).
- [ ] M038 Generic multiset duplicate preservation across all operators.
- [ ] M052 Lowering SQL plans into reusable dataflow fragments.
- [ ] M064 Recursive dataflow maintenance.
- [x] M064a Append-only incremental transitive reachability with cycle-safe positive deltas; arbitrary deletes and updates remain rebuild-only. See [INCREMENTAL_RECURSIVE_REACHABILITY.md](INCREMENTAL_RECURSIVE_REACHABILITY.md).
- [ ] M065 Incremental window-function maintenance.
- [x] M065a Append-only incremental `ROW_NUMBER`, `RANK`, and `DENSE_RANK` maintenance with atomic batch validation; arbitrary updates and retractions remain open. See [INCREMENTAL_RANK_WINDOW.md](INCREMENTAL_RANK_WINDOW.md).
- [x] M065l Opt-in mutable bounded `ROWS` frame maintenance with exact `INSERT`/`UPDATE`/`DELETE` differentials and affected-partition rebuilds; peer-aware `RANGE` frames and automatic planner selection remain open. See [INCREMENTAL_MUTABLE_FRAME_WINDOW.md](INCREMENTAL_MUTABLE_FRAME_WINDOW.md).
- [ ] M090 Independent compute and storage scaling.
- [ ] T042 Recovery-time parallel replay. A bounded single-key parallel replay
- [ ] T047 Synchronous replication with an explicit quorum. The public single-command path is now opt-in through `MonitoringOptions.WriteQuorum` / `CacheGRPCOptions.WriteQuorum`; atomic `BATCH` quorum semantics and rollback-free cluster-wide commit remain open.
- [ ] T103 Native FFI extension boundary.
- [ ] T150 Language-neutral client SDK coverage.

### M051c: Immutable Compiled SQL Template Reuse

Adopted. Static parameter-free compiled SQL handles reuse the immutable
rewritten template; parameterized and execution-local calls retain cloning.
Verification and measurements are recorded in
[COMPILED_TEMPLATE_REUSE.md](COMPILED_TEMPLATE_REUSE.md).

### M065m: Peer-Aware Incremental Numeric RANGE Windows

Adopted as an opt-in append-only capability for `COUNT(*)` and `SUM(int64)`.
It maintains inclusive numeric range bounds, emits exact peer replacement
differentials, and leaves the existing ROWS-frame and default paths unchanged.
See [INCREMENTAL_RANGE_WINDOW.md](INCREMENTAL_RANGE_WINDOW.md).
### M065n: Incremental `RANGE` `MIN`/`MAX`

Implemented peer-aware numeric `RANGE` extrema for `int64` values. A
monotonic deque keeps the current MIN or MAX without rescanning the frame;
expired entries are removed by sequence, and all rows in an equal-order peer
group receive the same result. NULL values and descending order are covered by
tests. This is intentionally limited to append-only monotonic input; general
late-data maintenance remains open.
### M065o: Incremental `RANGE` `COUNT(DISTINCT)`

Implemented exact peer-aware `COUNT(DISTINCT int64)` for bounded numeric
`RANGE` frames. A multiplicity map supports duplicate values and exact expiry
without a frame rescan; NULL, peer, ascending, descending, and atomic
validation cases are tested. This remains an append-only capability with
explicit monotonic ordering, not a general late-data arrangement.
### M065p: Incremental `RANGE` `AVG`

Implemented peer-aware `AVG(int64)` for bounded numeric RANGE frames. It reuses
checked incremental sum/count arithmetic, ignores NULL values, handles
descending input and peer replacement, and preserves atomic overflow failure.
The append-only monotonic-order contract remains explicit.
### M065q: Incremental `RANGE` `FIRST_VALUE`/`LAST_VALUE`

Implemented generic peer-aware numeric RANGE boundary values. FIRST_VALUE uses
bounded active-frame retention; LAST_VALUE keeps only the current peer group
and emits exact peer replacements. NULL-respecting values, descending order,
expiry, callback atomicity, and partitioned reference tests are included.
### M065r: Incremental `RANGE` `NTH_VALUE`

Implemented a narrow Materialize-style differential window extension for fixed
position NTH_VALUE over numeric RANGE frames. It keeps active rows in frame
order, retains only current-peer source rows for exact replacements, supports
NULL and descending semantics, and rejects late/out-of-order appends. It is
opt-in and append-only; arbitrary updates, deletes, dynamic positions, and
planner integration remain intentionally deferred. The benchmark and memory
tradeoff are recorded in [BENCHMARK.md](BENCHMARK.md).
### M065s: SQL packed numeric predicate kernel

Implemented the SQL-side CH-048 follow-up for packed columnar `int64` and
`float64` predicates. Direct numeric `WHERE` conjunctions prevalidate packed
columns once and compare their bytes without per-row interface boxing. Legacy,
malformed, NULL, and unsupported predicate paths retain the established
fallback. Seven-sample benchmarking recorded 1.14x lower CPU time and 21.5%
fewer allocations on the packed workload without a format or configuration
change.
