# ClickHouse, Materialize, And Tarantool Gap Catalog

This catalog contains 150 candidate ideas that are not currently represented
as adopted features in `hatrie_cache`: 50 from ClickHouse, 50 from Materialize,
and 50 from Tarantool. It was checked against `INSPIRATION.md`,
`ADOPTED_QUERY_ENGINE_IDEAS.md`, and the current package layout on 2026-09-10.

An item marked **Gap** is a candidate, not a promise that the feature is
appropriate for this repository. Nearby primitives are called out so an
implementation does not duplicate existing work. Existing behavior remains
the compatibility baseline. Any implementation must add a focused regression
test first, run a before/after benchmark with CPU, allocations, memory, and
wire/storage impact where applicable, and be rolled back when the measured
tradeoff is not justified.

## Selection Rules

- Do not count an idea as new when the repository already has the same behavior
  under another name.
- Keep product-specific operational machinery optional unless the feature is
  required for correctness or a measured default path win.
- Prefer bounded state, explicit ownership, deterministic output, and existing
  codecs/storage interfaces.
- Do not implement automatic sharding or per-partition journal durability in
  this catalog: the earlier partitioning decision explicitly keeps those as a
  proposal/deferred boundary until backup and recovery ordering are specified.
- A candidate may be rejected after measurement. Rejection is a valid result and
  must retain the benchmark and rationale.

## First Implementation Queue

| Priority | ID | Candidate | Initial decision | Required gate |
|---:|---|---|---|---|
| 1 | CH-G01 | External aggregation spill for bounded `GROUP BY` | Start with a test-first prototype behind an explicit option | Exact grouped output, cancellation cleanup, restart safety, peak RSS, spill bytes, and CPU versus the current memory-budget rejection path |
| 2 | CH-G02 | Unified external sort for generic ordered queries | Evaluate only after the spill lifecycle is understood | Stable ties, NULL/collation behavior, temporary-file cleanup, and end-to-end latency/bytes |
| 3 | M-G07 | Source lag and health status records | Low-risk observability candidate | No query-path overhead by default, bounded diagnostic text, and race coverage |
| 4 | T-G42 | Key-change notification watchers | Evaluate against existing subscriptions | No lost updates, bounded subscriber memory, cancellation, and fanout cost |

The first implementation is intentionally not marked adopted here. Its status
changes only after the red-green benchmark gate is complete.

## ClickHouse Ideas

Primary references: [ClickHouse SELECT reference](https://clickhouse.com/docs/en/sql-reference/statements/select),
[MergeTree table engines](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree),
[asynchronous inserts](https://clickhouse.com/docs/en/optimize/asynchronous-inserts),
[data skipping indexes](https://clickhouse.com/docs/en/optimize/skipping-indexes),
[projections](https://clickhouse.com/docs/en/data-modeling/projections), and
[system tables](https://clickhouse.com/docs/en/operations/system-tables).

| ID | Idea | Current gap in `hatrie_cache` | Value and implementation gate |
|---|---|---|---|
| CH-G01 | External aggregation spill for `GROUP BY` | Group memory governance can reject or fall back, but there is no bounded disk-backed aggregate state for generic SQL grouping. | Handles high-cardinality queries without an OOM or hard rejection. Gate exact NULL/type semantics, deterministic merge order, cancellation cleanup, disk quota, and peak RSS versus CPU. |
| CH-G02 | Unified external sort for generic `ORDER BY` | External sort exists only in selected bounded paths; there is no one reusable spill sorter for every compatible ordered query. | Keeps deep or wide sorts within memory limits. Gate stable ties, collation, temporary-file cleanup, and read/write amplification. |
| CH-G03 | Partial/final aggregate wire states | Local two-level aggregation exists, but there is no versioned partial aggregate envelope for transport between workers or nodes. | Reduces distributed aggregate transfer. Gate exact state merge, schema/version rejection, bounded state size, and CPU versus raw rows. |
| CH-G04 | `GROUPING SETS`, `ROLLUP`, and `CUBE` SQL syntax | Existing rollups and grouped operators do not provide the complete SQL grouping-set syntax and grouping-id semantics. | Covers common dimensional reports in one scan. Gate duplicate grouping rows, NULL grouping markers, ordering, and memory bounds. |
| CH-G05 | `WITH TOTALS` result section | Grouped SQL does not expose ClickHouse-style totals alongside ordinary grouped rows. | Avoids a second client query for a grand total. Gate filters, HAVING behavior, empty input, serialization shape, and backward-compatible result formats. |
| CH-G06 | `LIMIT ... WITH TIES` | Global and per-group limits exist, but no explicit boundary-tie extension returns all rows equal to the last ordered value. | Preserves ranking boundaries without client-side overfetch. Gate stable order, NULLs, composite keys, and bounded output controls. |
| CH-G07 | `QUALIFY` post-window filtering | Window-related support is partial and there is no post-window filter phase with its own SQL grammar. | Makes window queries composable without nested subqueries. Gate evaluation order, aliases, NULLs, and unsupported-window rejection. |
| CH-G08 | ASOF temporal join | Temporal tables and interval joins are separate boundaries; there is no nearest-prior timestamp join operator. | Supports event enrichment against the latest known dimension state. Gate duplicate timestamps, late rows, time zones, and indexed versus scan plans. |
| CH-G09 | Dedicated SEMI and ANTI join physical operators | Semi-join reduction exists as an optimization, but there is no explicit operator contract for left-semi and left-anti output semantics. | Reduces materialization for existence checks. Gate duplicates, NULL join keys, correlated predicates, and fallback equivalence. |
| CH-G10 | Grace hash join with disk partitions | Joins use in-memory or existing bounded paths; a disk-partitioned join for oversized inputs is not present. | Prevents memory spikes on large joins. Gate skew, recursive partitioning limits, cleanup, and CPU/IO tradeoff. |
| CH-G11 | Statistics-driven join algorithm selection | Query planning has narrow exact rewrites, not a calibrated selector among hash, merge, indexed, and spill joins. | Chooses faster plans across data shapes. Gate plan stability, stale stats, explainability, and no regression on small queries. |
| CH-G12 | Parallel-replica query execution | Partition reads are bounded and deterministic, but replica-level parallel execution with duplicate-work suppression is not present. | Improves availability and tail latency for replicated read workloads. Gate duplicate result elimination, consistency, cancellation, and network cost. |
| CH-G13 | Parameterized SQL plan cache | Result and projection caches exist, but parsed/optimized parameterized plans are not retained with schema-epoch invalidation. | Removes repeated parse/optimize work for hot templates. Gate schema changes, parameter types, bounded LRU memory, and stale-plan safety. |
| CH-G14 | Prepared-plan admission and eviction metrics | There is no first-class plan-cache status showing hits, misses, evictions, or memory. | Makes plan caching operable. Gate privacy, bounded labels, and zero overhead when disabled. |
| CH-G15 | JIT compilation for hot scalar expressions | Predicates use native/vectorized paths, but no hot-expression compilation or safe fallback exists. | Can reduce interpreter overhead on repetitive CPU-bound expressions. Gate compilation latency, code memory, determinism, and sandbox/security boundaries. |
| CH-G16 | Versioned serialized aggregate states | Aggregate combinators exist, but aggregate state persistence is not a versioned public storage/wire contract. | Enables durable rollups and cross-process merge. Gate version rejection, endian/type compatibility, corruption detection, and state-size growth. |
| CH-G17 | KLL or t-digest quantile state | Approximate aggregates exist in part of the codebase, but no measured mergeable quantile state with explicit error bounds is exposed. | Gives bounded-memory percentile aggregation. Gate deterministic merge policy, error reporting, NaN/NULL behavior, and accuracy versus memory. |
| CH-G18 | Quantile timing and error metadata | Query results do not carry approximation error or state-quality metadata for approximate quantiles. | Lets operators choose accuracy knowingly. Gate result compatibility and no metadata on exact aggregates. |
| CH-G19 | Fixed-precision Decimal physical type | Numeric typed columns focus on integer and floating values; no exact decimal storage and arithmetic contract is available. | Avoids money/scale rounding errors. Gate overflow, scale coercion, serialization, indexes, and CPU cost. |
| CH-G20 | IPv4 and IPv6 typed columns/functions | No dedicated IP physical type and containment/range function family is present. | Makes network analytics compact and indexable. Gate canonicalization, IPv4-in-IPv6 rules, NULLs, and text compatibility. |
| CH-G21 | UUID typed physical column | UUID values can appear dynamically, but there is no fixed-width typed-table UUID column and index path. | Reduces UUID storage and comparison cost. Gate textual/binary round trips, byte ordering, NULLs, and hash/range semantics. |
| CH-G22 | Enum8 and Enum16 columns | Schema validation has general types but no compact validated enum code storage. | Shrinks low-cardinality status fields and rejects invalid labels early. Gate schema evolution, unknown labels, wire compatibility, and dictionary memory. |
| CH-G23 | Persistent LowCardinality dictionaries across parts | Dictionary encoding is available for selected typed-table layouts, not as a persistent part-level dictionary lifecycle. | Avoids rebuilding dictionaries after reopen/merge. Gate dictionary churn, merge cost, code width changes, and recovery. |
| CH-G24 | Nested columns with independent subcolumn reads | Native arrays exist, but nested repeated structures do not expose ClickHouse-style independent subcolumns and offsets. | Reads only used nested fields and reduces allocation. Gate offsets, empty arrays, NULLs, and malformed layout rejection. |
| CH-G25 | Physical Map subcolumns | JSON paths are optimized, but typed `Map` values do not have a separate key/value subcolumn layout. | Speeds selective map-key reads. Gate duplicate keys, ordering, missing keys, and mutation maintenance. |
| CH-G26 | Dynamic/Object columns with schema evolution | JSON values are supported, but no bounded dynamic-column schema registry promotes stable paths into physical columns. | Provides flexible ingestion with typed hot paths. Gate path explosion, schema limits, type conflicts, and fallback correctness. |
| CH-G27 | Default-value suppression codec | Nullable bitmaps and adaptive encodings exist, but repeated default values are not represented with a dedicated sparse/default-value stream. | Reduces storage for sparse columns. Gate all-default/all-nondefault blocks, random access, and CPU versus raw encoding. |
| CH-G28 | Automatic granularity selection | Granule policy exists as an explicit control, not a storage-integrated per-part selector based on row width and predicate history. | Balances mark overhead and scan amplification automatically. Gate reproducibility, write cost, and bounded metadata. |
| CH-G29 | Persistent mark cache | Part cache policy exists, but there is no integrated cache of decoded mark/index ranges keyed by part generation. | Cuts repeated range-planning and index IO. Gate generation invalidation, memory bounds, and cache-thrash behavior. |
| CH-G30 | Uncompressed block cache | There is no reusable per-part uncompressed data-block cache with admission and eviction tied to query workload. | Avoids repeated decompression for hot analytical reads. Gate duplicate memory accounting, stale generation safety, and compression tradeoff. |
| CH-G31 | Automatic dependency-aware query result cache | A generic result cache exists, but SQL does not automatically cache eligible results with source dependency epochs and bounded admission. | Improves repeated identical reads. Gate mutation invalidation, user isolation, parameter privacy, memory, and stale-result rejection. |
| CH-G32 | Asynchronous `OPTIMIZE` and mutation jobs | Maintenance schedulers and index rebuilds exist, but table-level optimize/merge jobs lack a unified observable queue. | Lets operators schedule heavy maintenance without blocking commands. Gate cancellation, progress, durable intent, and resource budgets. |
| CH-G33 | Mutation coalescing and supersession | Separate mutations are not automatically combined when later work supersedes earlier work on the same key/part. | Reduces write amplification. Gate ordering, hooks, auditability, and cases where every mutation is externally observable. |
| CH-G34 | Mutation priority and kill control | Query cancellation does not provide a mutation-specific priority/cancel API. | Protects foreground traffic during backfills. Gate atomicity, partial progress reporting, and restart behavior. |
| CH-G35 | Part-level write quorum | Quorum controls exist at other boundaries, but immutable part publication has no explicit per-part replica quorum contract. | Makes durable analytic replicas observable before acknowledgement. Gate partial quorum failure, retry, and backup consistency. |
| CH-G36 | Keeper-style replicated metadata log | Topology and journals exist, but there is no coordination service for replicated part metadata, leases, and ordered metadata writes. | Enables stronger multi-node table coordination. Gate split-brain fencing, availability, and operational complexity before any default. |
| CH-G37 | Atomic manifest publication protocol | Persistent storage has checkpoints and restore, but no reusable manifest protocol for publishing multiple immutable parts atomically. | Makes multi-part snapshots and schema changes crash-safe. Gate fsync ordering, torn manifest recovery, and backward compatibility. |
| CH-G38 | Mmap-backed read-only parts | Zero-copy immutable transfer views exist, but persistent analytical parts do not expose validated mmap read views. | Reduces copy and heap pressure for cold scans. Gate file lifetime, truncation safety, address-space limits, and platform fallback. |
| CH-G39 | Object-store prefetch and read-ahead | Remote part references exist, but no bounded prefetch scheduler predicts ordered scan ranges. | Hides object-store latency for sequential scans. Gate cancellation, bandwidth budgets, cache pollution, and no-prefetch fallback. |
| CH-G40 | Working-set filesystem cache admission | Part cache policy is not integrated with query heat, part age, and storage-tier cost. | Keeps hot parts local without unbounded cache growth. Gate fairness, hysteresis, and accurate byte accounting. |
| CH-G41 | Complete part lifecycle system tables | System tables cover selected SQL state, not a full per-part view of rows, bytes, marks, mutations, merges, and tier/replica state. | Gives operators actionable storage visibility. Gate bounded snapshots, privacy, and no data-path locking. |
| CH-G42 | Per-operator memory tracker | Query governance has resource limits, but operators do not consistently report and cancel on local memory usage. | Prevents one aggregation/join operator from consuming the query budget. Gate accounting overhead, nested operators, and cancellation cleanup. |
| CH-G43 | Persistent redacted query logs | Query history is bounded and privacy-aware, but there is no durable redacted query log with rotation and retention policy. | Supports incident analysis across restart. Gate literal/secret redaction, disk budget, and opt-in security posture. |
| CH-G44 | Exportable trace context and span exporter | SDK-neutral spans exist, but there is no configurable OTLP/exporter transport and propagation contract. | Integrates query diagnostics with production tracing. Gate backpressure, secret handling, sampling, and disabled-path overhead. |
| CH-G45 | `EXPLAIN ESTIMATE` cost model | What-if advice exists, but normal `EXPLAIN` does not provide calibrated row, byte, CPU, and memory estimates for candidate plans. | Makes plan selection explainable before execution. Gate stale statistics, confidence labels, and no execution side effects. |
| CH-G46 | Histograms and NDV statistics objects | Source statistics include bounded exact metadata, but there are no persisted histograms or distinct-value sketches used by planning. | Improves selectivity and join decisions. Gate bounded error, refresh cost, invalidation, and memory. |
| CH-G47 | Workload-adaptive index granularity | No feedback loop changes index/skip granularity from observed predicate selectivity under an explicit budget. | Can reduce scan work for stable workloads. Gate oscillation, rebuild cost, deterministic limits, and rollback. |
| CH-G48 | `INSERT SELECT` without row materialization | SQL mutation and query paths do not share a zero-copy pipeline for inserting selected rows into typed storage. | Reduces CPU and transient heap for ETL-like writes. Gate transactional failure, source lifetime, and trigger visibility. |
| CH-G49 | Adaptive format negotiation for HTTP bulk input | Several codecs exist, but no authenticated endpoint negotiates format from schema, entropy, and client capability automatically. | Saves wire bandwidth for compatible clients. Gate downgrade safety, CPU budget, decompression limits, and explicit default-off behavior. |
| CH-G50 | Named logical table snapshots and freeze/restore | Backup and restore operate on cache/repository boundaries, not a named logical table snapshot with an immutable generation handle. | Enables point-in-time operational workflows. Gate cross-table consistency, retention, authorization, and recovery tests. |

## Materialize Ideas

Primary references: [Materialize concepts](https://materialize.com/docs/fundamentals/concepts/),
[sources](https://materialize.com/docs/concepts/sources/),
[indexes](https://materialize.com/docs/concepts/indexes/),
[arrangements](https://materialize.com/docs/get-started/arrangements/),
[sinks](https://materialize.com/docs/concepts/sinks/),
[views](https://materialize.com/docs/concepts/views/), and the
[system catalog](https://materialize.com/docs/reference/system-catalog/).

| ID | Idea | Current gap in `hatrie_cache` | Value and implementation gate |
|---|---|---|---|
| M-G01 | Kafka source connector with partition offsets | Offset tracking exists as an adapter primitive, but there is no first-class Kafka source lifecycle or partition consumer. | Enables durable streaming ingestion. Gate offset atomicity, rebalance behavior, backpressure, and dependency policy. |
| M-G02 | PostgreSQL logical CDC source | SQL sources are local/adapted; no PostgreSQL replication-slot or WAL-decoding source is present. | Brings transactional OLTP changes into incremental views. Gate slot cleanup, schema changes, reconnects, and security. |
| M-G03 | Debezium/upsert envelope source | CDC envelopes exist for downstream records, not a source-side Debezium/upsert decoder with key replacement semantics. | Handles updates from change streams without full rescans. Gate deletes, tombstones, duplicate offsets, and schema evolution. |
| M-G04 | Source snapshot barrier | Source reads do not expose a first-class snapshotting state that blocks dependent queries until a consistent initial load completes. | Prevents partial initial results. Gate timeout, failure recovery, and legacy resolver behavior. |
| M-G05 | Source schema evolution for add/drop columns | Schema compatibility checks exist for replication, but source-owned SQL schema evolution is not automatic. | Allows non-breaking upstream changes without downtime. Gate type widening, defaults, indexes, and old-client behavior. |
| M-G06 | Pause and resume source ingestion | Replication queues can pause, but there is no generic source pause contract that preserves offsets and frontiers. | Gives operators controlled maintenance windows. Gate no loss/duplication, bounded backlog, and restart behavior. |
| M-G07 | Source lag and health status records | Monitoring has broad metrics but no source-level lag, frontier, snapshot, retry, and last-error object. | Reduces diagnosis time. Gate bounded text, privacy, scrape cost, and default no-op overhead. |
| M-G08 | Connector retry and backoff policy | Remote read retries exist, but source connectors have no common exponential backoff, jitter, and circuit state. | Prevents reconnect storms. Gate deterministic tests with fake clocks and no hidden retry amplification. |
| M-G09 | Source event deduplication by offset or event ID | Idempotency is available for commands, not a general source event dedup state with bounded retention. | Handles at-least-once upstream delivery. Gate memory bounds, retention policy, and false duplicate rejection. |
| M-G10 | Cross-table source transaction grouping | Transaction grouping exists for selected SQL source operations, not a source connector that atomically publishes a multi-table CDC transaction. | Preserves relational consistency for incremental consumers. Gate partial transaction recovery and ordering. |
| M-G11 | Kafka materialized sink | SQL sink progress exists, but no Kafka producer, partitioner, or durable output contract is implemented. | Publishes maintained views to event consumers. Gate retries, ordering, exactly-once/idempotency, and backpressure. |
| M-G12 | PostgreSQL materialized sink | No sink writes maintained rows or changes to PostgreSQL. | Supports serving/replication workflows. Gate transaction batching, reconnects, schema mapping, and poison-row handling. |
| M-G13 | Redis materialized sink | No Redis sink lifecycle or key/value encoding exists. | Makes maintained state available to cache clients. Gate idempotent updates, deletes, TTL policy, and credentials. |
| M-G14 | Sink delivery retry queue | Durable mutation queues do not provide a generic per-sink delivery queue with retry state and poison handling. | Keeps transient destinations from blocking compute. Gate ordering, capacity, and operational visibility. |
| M-G15 | Sink backpressure and frontier coupling | A sink cannot currently advertise downstream capacity and hold its acknowledged frontier. | Prevents unbounded output memory or loss. Gate source progress, deadlock avoidance, and cancellation. |
| M-G16 | Cluster resource pools | SQL namespaces and memory budgets exist, but no Materialize-like isolated compute cluster owns sources, views, indexes, and ad-hoc work. | Separates noisy workloads. Gate scheduler fairness, memory accounting, and configuration complexity. |
| M-G17 | Cluster replica lifecycle and autoscaling | Topology replica controls are not a compute-cluster replica manager with scale-up/down and catch-up status. | Improves availability and capacity operations. Gate deterministic routing, no duplicate writes, and drain semantics. |
| M-G18 | Cluster-local index placement | Indexes are not explicitly owned by isolated compute pools with locality-aware query planning. | Avoids remote index assumptions and clarifies memory ownership. Gate plan invalidation and cross-cluster fallback. |
| M-G19 | Cross-cluster object dependency catalog | Views and indexes lack a catalog of cluster dependencies and resource ownership. | Supports safe migration and operator impact analysis. Gate atomic catalog updates and privacy. |
| M-G20 | Reuse-aware index creation planning | Shared arrangements exist, but creating a new index/view does not expose a full reuse decision and dependency explanation. | Avoids redundant maintained state. Gate deterministic explain output and creation-order behavior. |
| M-G21 | Hydration progress and ETA | Arrangement hydration can be bounded, but generic persisted view/index hydration has no progress contract. | Makes restart readiness observable. Gate no false readiness, cancellation, and bounded polling. |
| M-G22 | Pause/resume dataflow lifecycle | Incremental runners can be controlled, but there is no generic dataflow pause preserving frontiers and state. | Enables safe maintenance without dropping updates. Gate queued changes, resume ordering, and resource release. |
| M-G23 | Online materialized-view replacement | There is no atomic replacement of a maintained view while preserving dependents and avoiding a full service outage. | Enables compatible schema/query evolution. Gate dual maintenance cost, rollback, and dependency publication. |
| M-G24 | Indexed-view versus materialized-view DDL | The API has several explicit materialization primitives but no SQL DDL that distinguishes memory-local indexed views from durable materialized views. | Makes lifecycle and cost intent declarative. Gate parser compatibility and default behavior. |
| M-G25 | Arrangement key advisor | Index advisors cover SQL indexes, not differential arrangement keys with distribution and memory estimates. | Reduces arrangement state and skew. Gate source statistics, deterministic advice, and no automatic creation. |
| M-G26 | Adaptive logical compaction policy | Differential state has consolidation paths, but no policy tunes compaction by update rate, time frontier, and memory budget. | Controls long-lived update history cost. Gate exact multiplicities, CPU spikes, and cancellation. |
| M-G27 | Antichain/frontier API | Logical timestamps and frontiers exist in several boundaries, but no reusable antichain API governs multi-input progress. | Makes multi-source progress composition precise. Gate partial orders, empty frontiers, and deterministic serialization. |
| M-G28 | Global `AS OF` query consistency | Temporal tables support historical reads, but arbitrary multi-source SQL lacks one logical timestamp snapshot contract. | Prevents cross-source time skew. Gate unsupported sources, retention errors, and planner semantics. |
| M-G29 | Snapshot-free subscription mode | Durable subscriptions record an initial snapshot, but there is no explicit start-live-without-snapshot mode. | Supports consumers that already have state. Gate sequence/frontier semantics and missed-update warnings. |
| M-G30 | Differential subscription envelope | Subscription output does not expose a standardized `(row, logical time, diff)` envelope for retractions and multiplicity. | Lets consumers maintain multisets without full replacement snapshots. Gate type preservation, ordering, and progress. |
| M-G31 | Multi-source consistent snapshot | There is no coordinator that captures source versions/frontiers atomically across independent resolvers. | Makes initial joins and exports consistent. Gate blocking, source failure, and version retention. |
| M-G32 | Full catalog schemas and dependency metadata | SQL system tables expose selected state, not a complete Materialize-style catalog of schemas, sources, views, sinks, indexes, and dependencies. | Enables tooling and safe migrations. Gate bounded output and version stability. |
| M-G33 | Dataflow cardinality and arrangement `EXPLAIN` | Explain graphs exist, but they do not show estimated/observed cardinality, arrangement keys, or memory per operator. | Makes incremental plans diagnosable. Gate privacy, stats uncertainty, and no execution. |
| M-G34 | `EXPLAIN CREATE INDEX` reuse and memory report | What-if advisors do not provide the exact create-time reuse/dependency report for a maintained arrangement. | Prevents accidental duplicate state. Gate stale catalog handling and deterministic recommendations. |
| M-G35 | Per-cluster query admission | Query cancellation and namespace quotas exist, but admission is not scheduled against cluster-local CPU/memory pools. | Protects maintenance and serving workloads. Gate fairness, queue bounds, and cancellation. |
| M-G36 | Workload classes and priorities | No SQL-level workload class controls which source, compute, sink, or ad-hoc work receives priority. | Makes resource policy explicit. Gate starvation prevention and backward-compatible defaults. |
| M-G37 | Durable redacted query history | Query history is bounded, but it is not a durable redacted audit stream with restart retention. | Supports cross-restart incident review. Gate literal/secret redaction, quotas, and disk failure behavior. |
| M-G38 | Source/compute/sink metrics catalog | Metrics are exposed through monitoring APIs, not as a coherent SQL-readable metrics catalog tied to dataflow objects. | Allows SQL and dashboards to use one model. Gate cardinality and scrape overhead. |
| M-G39 | Logical replication publication/subscription | Replication APIs exist for cache state, not a Materialize-style logical publication of maintained SQL changes to another instance. | Supports downstream analytical replicas. Gate schema/version and exactly-once boundaries. |
| M-G40 | General monotonicity inference | Monotonicity helpers cover typed-table cases, not a planner-wide analysis of joins, filters, aggregates, and source envelopes. | Enables cheaper append-only maintenance safely. Gate retractions, NULLs, and conservative fallback. |
| M-G41 | Arrangement-only recovery | Hydration reads persisted state, but there is no public recovery mode that restores maintained arrangements without rereading an upstream source. | Shortens restart recovery. Gate checkpoint compatibility, missing history, and source version checks. |
| M-G42 | Arbitrary differential window frames | Incremental windows cover bounded append-only cases, not general frame bounds, retractions, or weighted updates. | Expands low-latency streaming SQL. Gate exact frame semantics, memory, and late data. |
| M-G43 | Late-data policy per dataflow | Late-data utilities exist, but no maintained dataflow declares allowed lateness, correction, and frontier policy as one contract. | Makes event-time behavior predictable. Gate retractions, retention, and operator reporting. |
| M-G44 | Temporal join compaction and eviction | Temporal joins do not have a common policy for retaining only time-relevant history after both inputs advance. | Bounds long-lived join state. Gate out-of-order data and exact historical queries. |
| M-G45 | Connection and secret resources | Authentication exists, but SQL does not expose named connections/secrets with scoped ownership and rotation. | Avoids embedding credentials in source definitions. Gate redaction, reload, and authorization. |
| M-G46 | Namespace and role hierarchy | Object namespaces exist conceptually, but there is no complete SQL catalog with roles, grants, ownership, and inheritance. | Supports multi-tenant operation. Gate default-deny behavior and migration compatibility. |
| M-G47 | Source connector transaction retry journal | Source offset tracking does not retain a durable connector transaction intent and retry outcome. | Prevents ambiguous external transaction recovery. Gate idempotent replay and bounded retention. |
| M-G48 | Backfill with frontier handoff | There is no first-class operation that backfills a new view/index from a snapshot and hands it into live updates at one frontier. | Enables online derived-state creation. Gate duplicate/missing updates, cancellation, and cutover atomicity. |
| M-G49 | Object lifecycle retention policies | Journal/projection retention exists, but no unified policy coordinates source, arrangement, sink, and subscription history retention. | Controls storage without breaking consumers. Gate dependency frontiers and operator preview. |
| M-G50 | Managed SQL object migrations | No versioned migration runner coordinates source, view, index, sink, and dependent replacement with a dry run and rollback plan. | Reduces operational deployment risk. Gate partial failure recovery, lock scope, and backward-compatible DDL. |

## Tarantool Ideas

Primary references: [Tarantool reference](https://www.tarantool.io/en/doc/latest/reference/),
[spaces](https://www.tarantool.io/en/doc/latest/book/box/space/),
[indexes](https://www.tarantool.io/en/doc/latest/book/box/box_space/),
[transactions](https://www.tarantool.io/en/doc/latest/book/box/transactions/),
[fibers](https://www.tarantool.io/en/doc/latest/book/box/atomic/),
[replication](https://www.tarantool.io/en/doc/latest/book/replication/), and
[sharding](https://www.tarantool.io/en/doc/latest/book/cartridge/cartridge_vshard/).

| ID | Idea | Current gap in `hatrie_cache` | Value and implementation gate |
|---|---|---|---|
| T-G01 | Tuple/space schema catalog | Typed values and SQL schemas exist, but there is no Tarantool-like named space with tuple format and field numbers. | Gives compact stable records and migration metadata. Gate schema validation, field numbering, and legacy JSON behavior. |
| T-G02 | Selectable memtx-style row engine | HAT-trie values and typed tables exist, but no separate in-memory tuple engine is selectable per collection. | Could improve predictable tuple access. Gate memory overhead and benchmark against current typed tables. |
| T-G03 | Vinyl-style LSM table engine | Pebble persistence exists, but no user-selectable table engine with Tarantool vinyl-like read/write tradeoffs. | Supports larger-than-RAM durable tables. Gate compaction, read amplification, and backup/recovery. |
| T-G04 | Memcs cache engine semantics | TTL and cache operations exist, but no explicit volatile cache engine with eviction and restart-loss semantics. | Makes cache-only durability intent clear. Gate eviction, memory accounting, and operator safety. |
| T-G05 | TREE secondary index iterator API | Ordered SQL indexes exist, but no generic tuple index iterator with first/next/after semantics. | Enables allocation-light ordered scans for embedded callers. Gate concurrent mutation snapshots and cursor invalidation. |
| T-G06 | HASH secondary index over tuple fields | HAT-trie lookup is not a general tuple-field hash index API with unique/non-unique modes. | Speeds exact tuple lookups. Gate collision handling, updates, and memory versus HAT keys. |
| T-G07 | Integrated R-tree space index | An R-tree data structure exists, but no tuple-space index maintenance and SQL planner integration use it automatically. | Makes spatial writes and queries one coherent contract. Gate update/delete correctness and rebuild cost. |
| T-G08 | BITSET secondary index | Exact packed bitmaps are available in hatDataStructure.BitsetIndex for compact uint32 slots; SQL planner integration remains opt-in and out of scope. | Makes low-cardinality membership selection cheap while retaining exact results; gate slot lifecycle, update/delete correctness, and memory crossover. |
| T-G09 | Functional secondary indexes | A reusable typed functional index is available in hatDataStructure.FunctionalIndex; SQL planner integration remains out of scope. | Accelerates normalized/domain-specific lookups. Gate deterministic extractors, stable IDs, and callback failure atomicity. |
| T-G10 | Automatic multikey tuple indexes | Array multikey SQL indexes exist, but tuple-space nested array expansion is not a general maintained index. | Supports repeated array membership queries. Gate duplicate elements, deletes, and index explosion limits. |
| T-G11 | Conditional tuple indexes | Partial SQL indexes exist, but there is no generic conditional space index with a predicate admission policy. | Shrinks index state for hot subsets. Gate predicate determinism, updates, and false negatives. |
| T-G12 | Index iterator modes and hints | Callers cannot request a specific index iterator strategy or inspect why another index was selected. | Makes embedded access predictable. Gate unsupported hint rejection and plan explainability. |
| T-G13 | Unique primary and secondary space constraints | Named schema constraints exist at selected boundaries, not a tuple-space uniqueness/index update contract. | Rejects duplicates before publication. Gate concurrent writes and multi-index rollback. |
| T-G14 | Tuple field operation updates | Mutations replace values or use SQL commands; no allocation-light field-level `+`, splice, and assignment operation batch exists for tuples. | Reduces read-modify-write cost. Gate overflow, type errors, and atomic rollback. |
| T-G15 | Tuple format validation and defaults | There is no compact positional tuple validator with default/generated field behavior. | Shrinks records and centralizes input validation. Gate missing fields, nullability, and evolution. |
| T-G16 | Versioned space migration manager | Schema compatibility exists, but no named migration plan tracks versions, preconditions, and rollback for spaces. | Makes online data evolution repeatable. Gate crash recovery and mixed-version clients. |
| T-G17 | Online `space.upgrade` equivalent | No background conversion can migrate existing records while serving compatible reads/writes. | Avoids maintenance downtime. Gate dual-format reads, progress, and cutover. |
| T-G18 | WAL sync mode policy | Journals have group commit, but no per-collection policy for synchronous, periodic, or disabled WAL semantics. | Makes durability/latency tradeoffs explicit. Gate unsafe default prevention and crash tests. |
| T-G19 | WAL compression and encryption | Binary journal/storage codecs exist, but no authenticated encrypted WAL envelope with key rotation is present. | Protects sensitive data at rest and reduces IO. Gate nonce/key management, corruption, and recovery. |
| T-G20 | Snapshot rotation policy | Backups exist, but no engine-level snapshot cadence/rotation policy coordinates snapshot size, WAL replay, and retention. | Bounds recovery time. Gate disk budgets and crash consistency. |
| T-G21 | Hot backup with WAL coordinate | Backup APIs do not expose one Tarantool-like snapshot plus exact WAL position for online bootstrap. | Enables consistent replica bootstrap. Gate concurrent writes, manifest integrity, and restore. |
| T-G22 | Incremental backup chain manager | Incremental repositories exist, but no generic chain planner verifies base generations and safe pruning for all storage engines. | Reduces backup bandwidth/storage. Gate missing base detection and restore rehearsal. |
| T-G23 | Join from snapshot plus WAL | Cluster join does not provide a standard snapshot transfer followed by journal catch-up and atomic activation. | Shortens node enrollment. Gate fencing, duplicate apply, and failure cleanup. |
| T-G24 | Synchronous replication quorum per write | Quorum APIs exist in selected paths, but there is no journal-wide per-write synchronous replica acknowledgement contract. | Gives explicit durability RPO. Gate latency, unavailable peers, and retry ordering. |
| T-G25 | Replica lag and RPO status | Replication metrics expose queue/wire state, but no node-level lag, applied sequence, and RPO status object covers every replica. | Improves failover decisions. Gate bounded metrics and no per-entry overhead. |
| T-G26 | Master-master conflict policy per space | Conflict resolution exists, but no space-level policy declaration selects last-write, source priority, or reject behavior. | Makes multi-writer behavior explicit. Gate deterministic clocks and auditability. |
| T-G27 | Raft leader and term state | Leader election/fencing primitives exist, but there is no replicated consensus log with terms, votes, and commit index. | Prevents split-brain metadata decisions. Gate quorum loss and persistence before deployment. |
| T-G28 | Automatic failover/election workflow | Topology controls are operator-led; no complete health-triggered failover promotes a safe replica automatically. | Reduces recovery time. Gate fencing, quorum, false positives, and operator override. |
| T-G29 | Persistent cluster membership | Topology membership is not a durable consensus-backed membership record with join/leave generations. | Makes restarts and discovery consistent. Gate stale membership and backup inclusion. |
| T-G30 | VShard bucket map and migration | Automatic sharding and bucket movement are explicitly deferred. | Could scale writes horizontally, but it must remain deferred until backup, ownership, and migration semantics are specified. Gate is a proposal and recovery design, not an implementation shortcut. |
| T-G31 | Replica-set health and read routing | Implemented as opt-in `hatTopology.ReplicaSetHealth`: bounded concurrent health observations, readiness/lag/maintenance filtering, locality ordering, and deterministic candidate selection for full-replica and sharded owners. It performs no probing or automatic failover. See [REPLICA_HEALTH.md](REPLICA_HEALTH.md). | Chooses healthy reads deterministically while leaving stale-health freshness, retries, failover, and write policy to the caller. |
| T-G32 | Net.box connection pool | Client APIs exist, but no pooled persistent peer connection with reconnect/backoff and bounded in-flight calls is provided. | Reduces connection setup and improves peer operations. Gate cancellation, auth, and pool shutdown. |
| T-G33 | IProto-like binary request multiplexing | Protobuf/gRPC and HTTP paths exist, but no compact multiplexed binary command protocol with correlation IDs targets embedded peers. | Reduces wire overhead and head-of-line blocking. Gate framing, compatibility, and auth. |
| T-G34 | Streaming binary responses | Implemented as opt-in `hatHttp.BinaryStreamWriter`/`BinaryStreamReader` and `StreamBinaryHTTPResponse`: bounded CRC32C frames, strict sequence validation, terminal end/error frames, context cancellation checks, and per-frame HTTP flushing. | Existing JSON/NDJSON defaults remain unchanged; a blocked arbitrary `io.Writer` cannot be interrupted by this package. See [TG34_STREAMING_BINARY_RESPONSES.md](TG34_STREAMING_BINARY_RESPONSES.md). |
| T-G35 | Stored Lua function calls | External extension/UDF boundaries exist, but no in-process stored function registry with Tarantool-like call semantics is available. | Offers low-latency trusted embedded procedures. Gate sandboxing, authorization, and panic isolation. |
| T-G36 | Function grants and roles | Authorization is present at service boundaries, but function/space-level grants are not a complete embedded policy model. | Enables least-privilege application roles. Gate deny-by-default and policy cache invalidation. |
| T-G37 | Real fiber scheduler | Cooperative schedulers and pipelines exist, but there is no runtime fiber abstraction with yield/resume stacks. | Could simplify high-concurrency embedded handlers. Gate benchmark against goroutines, cancellation, and stack memory. |
| T-G38 | Fiber channels and conditions | Typed pipeline channels exist, not general fiber-aware channels, conditions, semaphores, and wait groups. | Improves coordination for cooperative tasks. Gate deadlock detection and close semantics. |
| T-G39 | Fiber cancellation and deadlines | Context cancellation exists, but cooperative tasks do not share a fiber-native deadline and cancellation contract. | Prevents stuck handlers. Gate uninterruptible work and cleanup guarantees. |
| T-G40 | Fiber-local storage | No request/fiber-local context storage API exists outside ordinary Go contexts. | Avoids passing common metadata through every helper. Gate leak prevention and reuse. |
| T-G41 | Key-change watchers | Subscriptions exist at SQL/query boundaries, but no low-level watch API notifies consumers of exact key changes. | Enables cache invalidation and reactive clients. Gate no lost updates, bounded fanout, and ordering. |
| T-G42 | Watch filters and coalescing policy | No watcher can choose exact, coalesced, or prefix-filtered notifications with explicit loss semantics. | Controls notification cost for hot keys. Gate deterministic coalescing and caller-visible sequence gaps. |
| T-G43 | Queue consumer groups | Queues/delay queues exist, but no named consumer group partitions work with ownership and acknowledgements. | Supports horizontally scaled workers. Gate duplicate delivery, rebalance, and persistence. |
| T-G44 | Queue visibility timeout and acknowledgement | There is no leased queue item that returns after a worker crash unless explicitly failed. | Handles worker failure without loss. Gate clock behavior, retry limits, and dead-letter integration. |
| T-G45 | Session transaction settings | Transactions exist, but no connection/session defaults configure isolation, timeout, read-only, and synchronous durability per client. | Makes client policy reusable. Gate inheritance, reset, and authorization. |
| T-G46 | Read-only replica enforcement | Read routing exists, but a replica cannot declare and enforce read-only mode across every mutation path. | Prevents accidental stale-replica writes. Gate admin override and replication catch-up. |
| T-G47 | Transaction trigger phases | Atomic hooks exist, but there is no complete before/after-statement, before/after-commit phase matrix for every collection. | Enables predictable audit/index hooks. Gate ordering, recursion, rollback, and no hidden mutation. |
| T-G48 | Per-space runtime statistics | Monitoring is broad, but no exact per-space/index operation, byte, hit, miss, and latency counters are exposed. | Finds hot collections and bad indexes. Gate label cardinality and default allocation cost. |
| T-G49 | Slab fragmentation diagnostics | Memory reports cover runtime heap, not allocator slab classes, fragmentation, and reusable free-space detail. | Explains apparent memory leaks and sizing pressure. Gate platform portability and read-only access. |
| T-G50 | Protected/sandboxed LuaJIT execution | External extensions are isolated, but no trusted-language sandbox with instruction, memory, and syscall limits is available. | Could support user-defined procedures safely. Gate escape testing, deterministic limits, and process isolation. |

## Adoption And Commit Workflow

1. Add or update the focused test for one candidate and run its existing or new
   Makefile-backed test target. The test must fail for the missing behavior.
2. Implement the smallest bounded version behind an explicit option when the
   default path would change memory, CPU, ordering, durability, or security.
3. Run the focused test, package test, race test, vet, and benchmark. Record raw
   samples and CPU, heap, allocation, wire, storage, and recovery results in
   `BENCHMARK.md` when relevant.
4. Keep the change only when exact behavior is preserved and the measured
   benefit or operational value justifies its cost. Otherwise restore the
   pre-change behavior and record the rejection here.
5. Commit one isolated progress unit and push it only when the worktree can be
   proven not to include another session's staged or unstaged changes. A mixed
   shared worktree must not be force-cleaned to manufacture a commit.

Current status: the catalog is complete. `CH-G01` external grouped aggregation
now also handles direct composite group keys with bounded spill records;
`CH-G02` external sort, and `CH-G05` grouped `WITH TOTALS` now have passed
red-green benchmark gates; `CH-G05` is documented in `CHG05_WITH_TOTALS.md`.
`M-G07` now has an implemented bounded source-health registry
 and opt-in Prometheus metrics; first-class connector lifecycle and retry
 scheduling remain out of scope. `M-G29` now has an opt-in snapshot-free mode for
 regular query subscriptions; `M-G38` now has an opt-in read-only `system.metrics`
 catalog backed by telemetry or a caller provider; durable-log integration,
 differential envelopes, and automatic frontier handoff remain out of scope.
 `T-G42` has an implemented exact-key, bounded, lossless watcher baseline;
 coalescing and prefix-filter policies remain deferred until their loss semantics
 are designed. `T-G43` and `T-G44` now have an in-process consumer-group queue
 with explicit acknowledgements and visibility-timeout redelivery; durable
 cross-process ownership remains out of scope. `T-G08` exact compact-slot bitset
 indexes and `T-G09` typed functional secondary indexes are now implemented as
 reusable hatDataStructure primitives; SQL planner integration remains opt-in.
