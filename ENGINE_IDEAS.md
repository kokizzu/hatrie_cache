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
| CH-004 | `FINAL` read semantics | No query-time reconciliation of replacing or collapsing versions. | High |
| CH-005 | Lightweight delete bitmap | Deletes are not represented as compact immutable bitmaps over stored parts. | High |
| CH-006 | Mutation dependency queue | No durable dependency-aware queue for ALTER/DELETE mutations. | High |
| CH-007 | Row TTL | No declarative expiration of old rows by event or processing time. | Medium |
| CH-008 | Column TTL | No independent removal or masking of expired wide columns. | Medium |
| CH-009 | TTL rollup | No background aggregation of expired detail rows into coarser summaries. | High |
| CH-010 | Materialized/default columns | No stored expression column maintained during writes with schema validation. | Medium |
| CH-011 | General projection DDL | Only narrow cached ordering projections exist; no user-defined maintained projection. | High |
| CH-012 | Projection advisor | No cost-based recommendation comparing projection maintenance with query savings. | Medium |
| CH-013 | Query condition cache | No cache of reusable predicate outcomes for stable part/key conditions. | Medium |
| CH-014 | Uncompressed hot-data cache | No cache that stores decoded hot ranges while preserving compressed storage. | Medium |
| CH-015 | Filesystem cache admission | No admission/eviction policy for deciding which persistent ranges deserve RAM. | Medium |
| CH-016 | Asynchronous insert queue | Writes cannot be acknowledged before bounded background batching. | Medium |
| CH-017 | Async-insert deduplication | No idempotency token ledger for safely retrying queued inserts. | Medium |
| CH-018 | Insert quorum | No write acknowledgement policy requiring a configured replica quorum. | High |
| CH-019 | Replicated-part checks | No per-part checksums and replica consistency repair workflow. | High |
| CH-020 | Zero-copy part sharing | No remote part registration that avoids copying immutable storage between replicas. | High |
| CH-021 | Object-storage tiering | No hot/local and cold/object-storage tier with transparent reads. | High |
| CH-022 | Incremental part backup | Backup does not yet persist a content-addressed part manifest and changed-part set. | Medium |
| CH-023 | Selective partition restore | Restore cannot target only named tables or partitions with dependency checks. | Medium |
| CH-024 | Detach/attach parts | No operator API to quarantine immutable parts and attach verified replacements. | Medium |
| CH-025 | Merge-pool prioritization | Compaction lacks a user-visible priority scheduler balancing freshness and space. | Medium |
| CH-026 | Merge selector policies | No configurable size-tiered or time-aware merge selector. | Medium |
| CH-027 | Background-task observability | Partially adopted: `CompactionScheduler.Stats()` exposes queue depth, in-flight work, and success/failure counters; task-specific bytes, age, and TTL metrics remain provider-owned. | Low |
| CH-028 | Query `max_threads` setting | Worker count is not exposed as a stable SQL/session setting with admission checks. | Low |
| CH-029 | User/key quotas | No rolling per-user query/bytes/CPU quota enforcement. | Medium |
| CH-030 | Query complexity limits | Partially adopted: SQL options already bound rows, joins, results, bytes, skew, and now total `GROUP BY` keys; per-expression and CPU quotas remain future work. | Medium |
| CH-031 | Persistent query log | Implemented as an opt-in privacy-safe newline-delimited JSON log attached to `SQLQueryManager`; terminal status survives process restart without retaining SQL text, sources, parameters, or cancellation reasons. | Low |
| CH-032 | Query profiler samples | No sampled operator CPU/blocking profile attached to a query ID. | Medium |
| CH-033 | Distributed query fan-out | One query cannot plan and merge reads from multiple independent nodes. | High |
| CH-034 | Parallel replicas | No coordinated replica reads that divide ranges and merge ordered results. | High |
| CH-035 | Remote shard pruning | No shard-level predicate routing before distributed execution. | High |
| CH-036 | `ASOF JOIN` | Partially adopted: constrained `ASOF [LEFT] JOIN` with one equality and one temporal inequality, keyed right-side buckets, and binary search. | High |
| CH-037 | `ARRAY JOIN` | No row-expanding array join operator with SQL NULL semantics. | Medium |
| CH-038 | Aggregate combinators | `COUNT_IF`/`COUNTIF`, numeric `*_IF`, and `ARGMAX_IF`/`ARGMIN_IF` are adopted through the existing filter state; `OrNull`, `State`, and `Merge` variants remain absent. | Medium |
| CH-039 | Approximate distinct/quantile sketches | Partially adopted: global `APPROX_COUNT_DISTINCT` and `APPROX_PERCENTILE` queries now feed bounded sketches while streaming; grouped, top-k, and other complex shapes remain on the materialized evaluator. | Medium |
| CH-040 | `argMax`/`argMin` aggregates | Implemented for ordinary, grouped, filtered, and window aggregates, with a constant-state stream fast path for eligible global scans. | Low |
| CH-041 | `GROUPING SETS`/`ROLLUP`/`CUBE` | Partially adopted: existing multi-level expansion now supports `GROUPING(expr)` identifiers folded per branch; native one-pass grouping and multi-argument `GROUPING_ID` remain open. | High |
| CH-042 | Sampling key execution | `SAMPLE` is not a storage-aware deterministic sampling stage. | Medium |
| CH-043 | Gap filling/interpolation | No ordered time-bucket fill with explicit interpolation behavior. | Medium |
| CH-044 | JSON dynamic subcolumns | No path-level physical extraction and pruning for semi-structured rows. | High |
| CH-045 | Array/map subcolumn pruning | No read planner that loads only referenced nested subcolumns. | Medium |
| CH-046 | Native wire protocol framing | No ClickHouse-style typed block protocol with explicit column framing and progress. | High |
| CH-047 | Parallel format parsing | Large RowBinary payloads now index row boundaries once and decode independent ranges in parallel; small and single-core inputs remain serial. CSV/JSON block parsing remains open. | Medium |
| CH-048 | SIMD/generic predicate coverage | Existing `hatPredicate` masks cover typed int64 batches, and SQL direct numeric and packed-boolean predicates now use byte-oriented kernels; string and broader predicate shapes remain open. | Medium |
| CH-049 | Refreshable external dictionaries | Adopted as an opt-in `hatSql` registry with immutable atomic snapshots, manual/background refresh, bounded staleness, and `DICT_GET`/`DICT_GET_OR_DEFAULT`/`DICT_HAS`. | Done |
| CH-050 | Named settings collections | No versioned named query/storage profiles that can be applied atomically. | Low |

## Materialize candidates

| ID | Candidate not yet complete here | Current gap and likely value | Initial risk |
| --- | --- | --- | --- |
| MZ-001 | Durable persist shards | No durable collection shard abstraction that can hydrate without rereading the source. | High |
| MZ-002 | Snapshot frontier gating | Source reads do not expose a public readiness frontier that blocks until a consistent snapshot. | Medium |
| MZ-003 | Hydration progress and resume | Historical state rebuild has no public byte/record progress or resumable checkpoint. | Medium |
| MZ-004 | Logical compaction controls | No per-collection policy for compacting historical updates after a safe frontier. | High |
| MZ-005 | Since/upper frontier introspection | No API exposes lower and upper time frontiers for each maintained object. | Medium |
| MZ-006 | Antichain timestamps | No partially ordered timestamp frontier for concurrent partitions. | High |
| MZ-007 | Stale-read rejection | No query contract that rejects a read below a requested freshness frontier. | Medium |
| MZ-008 | `AS OF` query execution | Historical query reads cannot bind to a logical timestamp across sources. | High |
| MZ-009 | Temporal validity filters | No first-class valid-from/valid-to relation semantics with frontier-aware pruning. | High |
| MZ-010 | `TAIL`/`SUBSCRIBE` changefeed | SQL clients cannot subscribe to signed incremental result changes. | High |
| MZ-011 | Sink connector API | No durable outbound sink abstraction for Kafka, files, or HTTP streams. | High |
| MZ-012 | Exactly-once sink checkpoints | No atomic coupling between output offsets and maintained result frontiers. | High |
| MZ-013 | Source connector checkpoints | Ingestion does not expose durable source offsets tied to snapshot progress. | High |
| MZ-014 | Upsert-source consolidation | No generic source operator that collapses key updates while retaining correct deletes. | Medium |
| MZ-015 | CDC envelope normalization | No common insert/update/delete envelope adapter for external change streams. | Medium |
| MZ-016 | Source schema evolution | No additive/drop-column compatibility plan for live source versions. | High |
| MZ-017 | Parallel source hydration | Snapshot restoration cannot split independent source ranges across workers. | Medium |
| MZ-018 | Compute/storage separation | SQL workers cannot scale independently from durable maintained state. | High |
| MZ-019 | Per-cluster resource isolation | No named compute pools with independent CPU/memory admission. | Medium |
| MZ-020 | Worker scaling coordination | No online worker resize protocol that preserves frontier correctness. | High |
| MZ-021 | Replica hot handoff | No ready replica transfer that avoids a cold query-state rebuild. | High |
| MZ-022 | Index hydration readiness | Index creation has no public ready frontier that callers can await. | Medium |
| MZ-023 | `IN CLUSTER` index placement | Indexes cannot be assigned to an isolated compute pool. | Medium |
| MZ-024 | Automatic arrangement key selection | No planner explains or chooses a compact key for shared maintained state. | Medium |
| MZ-025 | Arrangement sharing by logical key | Existing sharing is definition-based; no canonical equivalence for semantically equal plans. | Medium |
| MZ-026 | Dictionary arrangement compression | Repeated arrangement strings are not transparently dictionary encoded. | Medium |
| MZ-027 | Arrangement memory telemetry | Implemented as an explicit read-only stats API reporting per-arrangement distinct values, bounded retained-byte estimates, checkpoints, source sequence, changelog compaction watermark, and dictionary group-order rebuild counts. | Low |
| MZ-028 | Adaptive arrangement compaction | No feedback loop that changes compaction cadence from memory and update rates. | Medium |
| MZ-029 | Spillable arrangements | Large maintained indexes cannot spill cold state to bounded local storage. | High |
| MZ-030 | Differential join delta maintenance | Joins do not expose a generic signed incremental join operator for updates/deletes. | High |
| MZ-031 | Skew-aware join exchange | No runtime detection and mitigation for hot join keys. | High |
| MZ-032 | Late-data reclocking | No operator that maps event-time updates into a controlled processing-time frontier. | High |
| MZ-033 | Timestamp oracle | No globally coordinated logical timestamp allocator across independent writers. | High |
| MZ-034 | Generic negative-diff operators | Signed retractions are not supported by every SQL operator. | High |
| MZ-035 | Multiset preservation everywhere | Duplicate multiplicities are not retained consistently across all operators. | High |
| MZ-036 | Recursive fixpoint scheduler | Recursive dataflow lacks a general iterative frontier scheduler. | High |
| MZ-037 | Incremental Top-K maintenance | Ordered top-k results are not maintained under arbitrary signed updates. | High |
| MZ-038 | Incremental order arrangement | No reusable ordered arrangement serves repeated range/order queries. | High |
| MZ-039 | General incremental distinct | Distinct maintenance is not a general relation operator for arbitrary updates. | Medium |
| MZ-040 | Incremental percentile | Percentiles are not maintained with a bounded or mergeable differential sketch. | High |
| MZ-041 | View freshness SLA | Implemented as optional per-task max-staleness status for managed materialized-view and rollup refreshes; existing scheduling remains unchanged by default. | Medium |
| MZ-042 | Dependency invalidation graph | Implemented as an automatic reverse source-to-view index for `MaterializedViews.RefreshChanged`; affected candidates are deduplicated and still sorted/published atomically. | Medium |
| MZ-043 | Transactional DDL dependencies | DDL cannot atomically create/alter a source, view, index, and dependent sink plan. | High |
| MZ-044 | Costed dataflow explanation | Explain output lacks arrangement cost, frontier, and memory estimates. | Medium |
| MZ-045 | Workload plan equivalence | Query fingerprints are not linked to canonical plan and arrangement reuse decisions. | Low |
| MZ-046 | Frontier-aware cancellation | A query cannot cancel after a specified freshness or result frontier is reached. | Low |
| MZ-047 | Session compute routing | Client sessions cannot choose a named compute cluster for an operation. | Medium |
| MZ-048 | Connector secret rotation | Source/sink credentials cannot rotate without stopping the maintained dataflow. | Medium |
| MZ-049 | Exactly-once snapshot export | Export has no frontier-bound manifest that can resume without duplicate rows. | High |
| MZ-050 | Timeline branching and replay | There is no isolated branch of maintained state for deterministic what-if replay. | High |

## Tarantool candidates

| ID | Candidate not yet complete here | Current gap and likely value | Initial risk |
| --- | --- | --- | --- |
| TT-001 | Automatic vshard bucket rebalancing | Partition plans exist, but no automatic data movement and ownership convergence. | High |
| TT-002 | Bucket ownership consensus | Partition ownership is not committed through a consensus-backed metadata log. | High |
| TT-003 | Router failover route cache | No client/router route cache with health-aware retry and invalidation. | Medium |
| TT-004 | Synchronous batch replication quorum | Single writes and batches do not provide rollback-free cluster-wide quorum commit. | High |
| TT-005 | Raft configuration state | No consensus-backed configuration and membership state machine. | High |
| TT-006 | Hot-standby WAL catch-up | No read-only standby that continuously replays and can be promoted without restore. | High |
| TT-007 | Snapshot-plus-WAL join | A joining node cannot hydrate from a snapshot and then apply a bounded WAL delta automatically. | High |
| TT-008 | Incremental snapshot chains | Snapshots are not content-addressed deltas with a verified parent chain. | Medium |
| TT-009 | Backup manifest checksums | Backup output lacks a durable file manifest with per-file hashes and sizes. | Low |
| TT-010 | Selective space backup | Operators cannot back up only named logical spaces/data structures. | Medium |
| TT-011 | Point-in-time incremental restore | Recovery cannot replay a verified journal only up to a timestamp or sequence. | Medium |
| TT-012 | Per-space storage engine choice | A logical data structure cannot independently choose memory and LSM persistence policies. | High |
| TT-013 | Vinyl range tuple cache | Storage has no range-aware cache that retains only hot key intervals. | High |
| TT-014 | Vinyl compaction throttling | Compaction does not expose adaptive disk/latency throttles and backpressure. | Medium |
| TT-015 | Vinyl read/write thread tuning | Read and compaction workers lack independent bounded runtime configuration. | Medium |
| TT-016 | Disk-space reserve admission | Implemented as an opt-in filesystem free-space reserve for LevelDB/Pebble full, key, dirty, and generation saves; default remains disabled. | Low |
| TT-017 | Run-level Bloom filters | Implemented as an opt-in native LevelDB/Pebble Bloom prefilter for new persistent runs and replication-outbox tables; default remains `0` because the measured warm workload adds 11.7%-14.4% storage with no general CPU win. | Medium |
| TT-018 | Page-index residency policy | Page indexes have no explicit memory budget and eviction metrics. | Medium |
| TT-019 | Covering secondary indexes | Secondary postings cannot retain selected payload fields to avoid primary lookups. | Medium |
| TT-020 | Generic multi-part TREE ranges | Public indexes do not expose efficient partial-key range scans across all structures. | Medium |
| TT-021 | RTREE spatial index | No multidimensional geographic index and bounding-box query API exists. | High |
| TT-022 | BITSET index | No bitmap index for low-cardinality integer membership. | Medium |
| TT-023 | HASH equality index | No dedicated hash index path for exact unique/non-unique equality lookups. | Low |
| TT-024 | Full-text phrase/position index | `CONTAINS_PREFIX` now uses an opt-in sorted token-key sidecar; token positions and phrase search remain absent. | High |
| TT-025 | Online uniqueness validation | Unique index creation has no staged validation before atomic publication. | Medium |
| TT-026 | Versioned tuple format | Stored rows have no schema version and migration decoder boundary. | High |
| TT-027 | Generated columns | No write-maintained expression columns with dependency validation. | Medium |
| TT-028 | Upsert conflict handlers | Public mutation commands lack declarative merge-on-conflict callbacks or policies. | Medium |
| TT-029 | General before/after replace triggers | Trigger scope is not a complete pre/post mutation lifecycle for every command. | Medium |
| TT-030 | Transactional DDL | Schema/index changes cannot be atomically grouped with data mutations. | High |
| TT-031 | Stream API | No request stream abstraction batches commands while preserving ordered responses. | Medium |
| TT-032 | IProto-style multiplexing | Wire clients cannot multiplex independent requests with bounded backpressure. | High |
| TT-033 | Fiber scheduler quotas | Cooperative tasks lack per-tenant CPU and queue budgets. | Medium |
| TT-034 | Cooperative task cancellation | Background fibers do not share a standard cancellation token and drain state. | Low |
| TT-035 | Per-request deadlines | Command APIs lack a consistent deadline propagated through storage and replication. | Low |
| TT-036 | Storage `box.stat` equivalent | Partially adopted: `CompactionScheduler.Stats()` adds low-overhead maintenance queue and outcome counters; storage-engine operation, page, cache, and WAL counters remain provider-owned. | Low |
| TT-037 | Audit log | No append-only operator/security audit stream with redacted command metadata. | Medium |
| TT-038 | Roles and grants for commands | Command access is not modeled as a per-operation role/privilege matrix. | Medium |
| TT-039 | Transparent credential rotation | Authentication credentials cannot rotate with overlapping validity and no restart. | Medium |
| TT-040 | Space changefeed | Clients cannot subscribe to committed row changes by logical space. | High |
| TT-041 | Crash-safe index resume | An interrupted index build has no durable unit checkpoint independent of the queue. | Medium |
| TT-042 | Restore resume checkpoints | A large restore cannot resume from a verified per-file/per-range checkpoint. | Medium |
| TT-043 | Maintenance read-only mode | No explicit mode rejects writes while allowing health, backup, and read traffic. | Low |
| TT-044 | Schema migration dry run | Implemented as importable `hatSchema.Preview`, which validates a migration on an independent schema copy without publication. | Low |
| TT-045 | Tuple-level compression | Individual large values/tuples lack transparent compressed storage with size thresholds. | Medium |
| TT-046 | Slab/memory accounting | No per-structure allocator accounting and bounded eviction/admission telemetry. | Medium |
| TT-047 | Expiration wheel | TTL cleanup has no hierarchical timer wheel for low-overhead mass expiration. | Medium |
| TT-048 | Queue/priority space primitive | No durable priority queue data structure with claim, retry, and visibility timeout. | Medium |
| TT-049 | Pessimistic row locks | No `SELECT FOR UPDATE`-style lock lease for callers that need serialized reads/mutations. | High |
| TT-050 | SQL planner statistics | Partial: explicit source-versioned `ANALYZE` statistics now feed what-if planning; durable on-disk statistics and a full cost model remain. | Medium |

## Selection order

The first implementation candidates beyond the adopted rows should be additive
and measurable: persistent backup manifests, background task
metrics, source-versioned SQL planner statistics, and bounded TTL expiration. Consensus,
distributed fan-out, durable dataflow state, and automatic repartitioning need
separate designs because they affect backup, recovery, and correctness across
nodes.
