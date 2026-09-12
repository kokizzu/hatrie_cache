# Product Idea Gaps

This is the current implementation queue for ideas compared with ClickHouse,
Materialize, and Tarantool. It contains 50 candidate gaps for each product.
The audit was performed against the exported Go packages, SQL surface, server
commands, monitoring APIs, backup/restore paths, and current documentation on
2026-09-12. A row is retained only when the repository has no complete,
end-to-end equivalent; a specialized or opt-in partial implementation is
called out in the gap column.

These are candidates, not promises. Each implementation must add a focused
test first, run the failing test, implement the smallest complete contract,
run package/race/vet/full verification, and benchmark the intended workload.
Features stay opt-in unless the measured default path is unchanged. A feature
is removed from this file or marked adopted only after its behavior and
tradeoffs are documented and its commit is published.

## ClickHouse

| ID | Candidate | Current gap | Adoption gate |
|---|---|---|---|
| CH-U01 | Durable asynchronous-insert deduplication | `hatPipeline.AsyncBatcher` batches values in memory, but the server has no durable insert-ID ledger that makes retried asynchronous inserts idempotent. | Crash/restart replay, bounded ledger memory, expiry, and duplicate-byte measurements. |
| CH-U02 | Unified external `ORDER BY` spill | External sorting is not one reusable path for every compatible SQL ordered query. | Stable ties, NULL/collation behavior, cancellation cleanup, disk quota, and RSS versus CPU. |
| CH-U03 | Versioned partial aggregate wire states | Aggregate states can be merged locally, but there is no versioned portable envelope for worker/node transfer. | Exact merge, schema/version rejection, bounded state size, and wire-size benchmark. |
| CH-U04 | External `DISTINCT` spill | High-cardinality distinct operations do not have a bounded disk-backed set path. | Exact type/NULL semantics, duplicate elimination, cleanup, and memory ceiling. |
| CH-U05 | External window-function state | Large window partitions lack a generic spillable execution path. | Frame correctness, stable ordering, cancellation, and temporary-file limits. |
| CH-U06 | Lightweight delete bitmap | SQL deletes do not use a compact row-existence mask that postpones physical rewrite. | Read correctness, update/delete interaction, compaction, and retained-byte crossover. |
| CH-U07 | Mutation wait/status lifecycle | Mutations do not expose a durable operation ID with progress, completion, and failure inspection. | Restart behavior, idempotent polling, bounded history, and authorization. |
| CH-U08 | Automatic SQL result-cache wiring | The generic epoch-validated result cache is not automatically selected for compatible SQL queries. | Dependency invalidation, parameter isolation, stale-read policy, and hit/miss benchmark. |
| CH-U09 | Persisted query-result cache | Query results are not retained across process restart with schema and source-version validation. | Corruption recovery, privacy boundaries, quotas, and cold-start tradeoff. |
| CH-U10 | Feedback-driven projection selection | Projection/index advice is present, but observed cost feedback does not automatically improve future selection. | Deterministic bounded history, no plan instability, and disabled-by-default overhead. |
| CH-U11 | Automatic data-skipping-index selection | Compatible skip indexes are not chosen and maintained from workload evidence as a complete lifecycle. | False-negative proof, rebuild scheduling, and memory/CPU budget. |
| CH-U12 | Background skip-index rebuild queue | Index rebuild requests lack a shared prioritized queue with cancellation and progress. | Concurrent writes, retry behavior, disk quota, and operator visibility. |
| CH-U13 | Phrase-aware inverted index | Token Bloom filtering exists, but exact phrase matching and ranked token postings are absent. | Unicode/token semantics, update cost, memory crossover, and bounded ranking. |
| CH-U14 | Join-derived runtime filter propagation | Runtime Bloom filtering exists only in selected paths, not as a general planner/executor filter exchange. | Exact fallback, filter lifetime, false-positive CPU cost, and cancellation. |
| CH-U15 | Vectorized decimal kernels | Native SIMD coverage does not include decimal arithmetic and comparisons. | Overflow/rounding parity, architecture fallback, and allocation-free benchmark. |
| CH-U16 | Adaptive low-cardinality dictionaries | String dictionaries are selected in some layouts, but dictionary admission and eviction are not adaptive to observed cardinality. | Deterministic encoding, update churn, memory threshold, and decode cost. |
| CH-U17 | Compressed `IN`-set representation | Large constant membership sets lack a planner-selected compact representation with exact fallback. | Type/NULL behavior, build threshold, memory, and lookup throughput. |
| CH-U18 | Composite primary-mark pruning | Sparse primary pruning covers selected numeric fields, not composite ordered marks with tuple bounds. | Lexicographic/NULL semantics, unsorted fallback, and selective/full-range benchmark. |
| CH-U19 | General `PREWHERE` syntax | Early filtering is implemented internally, but users cannot explicitly request or inspect a general `PREWHERE` stage. | Projection semantics, error order, explain output, and compatibility. |
| CH-U20 | Array/JSON late materialization | Nested values can use native layouts, but generic queries do not defer expensive array/JSON materialization until after selection. | Exact row alignment, malformed-value behavior, and allocation benchmark. |
| CH-U21 | Streaming `JSONEachRow` and CSV ingestion | Inserts do not expose bounded streaming parsers for common columnar text formats. | Malformed-row policy, limits, backpressure, and bytes/CPU versus materialized parsing. |
| CH-U22 | Direct columnar append ingestion | SQL inserts do not accept a validated columnar batch without row-by-row map conversion. | Schema/type checks, atomic failure, NULLs, and append throughput. |
| CH-U23 | Async-insert queue status and flush API | The new async batcher has stats, but there is no server-level queue registry and operator flush endpoint. | Authentication, queue quotas, graceful drain, and no default worker. |
| CH-U24 | Per-table memory budgets | Resource governance is not enforced as an exact per-table allocation budget across scans, indexes, and background work. | No false rejection, nested query attribution, and low-overhead counters. |
| CH-U25 | Per-query spill disk quotas | Memory limits exist in selected SQL paths, but a query cannot reserve and enforce one spill-byte budget. | Cleanup after crash/cancel and concurrent quota fairness. |
| CH-U26 | Operator memory profiles | Query traces expose timings and row counts, not a complete bounded peak/retained allocation profile per operator. | Low sampling overhead, privacy, and deterministic reports. |
| CH-U27 | Priority merge scheduler | Background compaction/merge work lacks a shared priority policy for latency-sensitive parts. | Starvation prevention, stable scheduling, and write amplification. |
| CH-U28 | Disk-I/O merge throttling | Compaction does not coordinate with measured disk bandwidth and foreground latency. | Portable metrics, no deadlock, and tail-latency benchmark. |
| CH-U29 | TTL tier movement | TTL can expire values, but it does not move eligible parts between hot/warm/cold placement tiers. | Atomic placement metadata, recovery, and movement bandwidth. |
| CH-U30 | Remote-part prefetch cache | Remote immutable part references exist, but there is no bounded local read-ahead cache keyed by part/column. | Cache admission, eviction, integrity, and read-amplification measurement. |
| CH-U31 | Multipart remote-part upload | Remote part references do not provide resumable, checksummed multipart upload coordination. | Retry safety, orphan cleanup, and bandwidth/CPU accounting. |
| CH-U32 | Remote-part garbage collection | Unreferenced remote parts are not discovered and deleted from a manifest-aware sweep. | Reachability safety, dry-run plan, retention, and operator authorization. |
| CH-U33 | Quorum part publication | Replication quorum APIs do not provide a part-level publish/acknowledge contract before visibility. | Fencing, unavailable peers, retry ordering, and latency. |
| CH-U34 | Idempotent mutation retries | SQL mutation retry does not have a durable mutation ID and result record spanning client/server retries. | Exact-once result semantics, conflict behavior, and retention. |
| CH-U35 | `OPTIMIZE`/merge-control command | Operators cannot request a bounded targeted part merge and observe its completion. | No unbounded memory use, cancellation, and write throttling. |
| CH-U36 | Complete `system.parts` catalog | Monitoring exposes selected part state, but SQL lacks a complete stable catalog of all part layout/checksum/retention fields. | Bounded output, secret-free fields, and schema stability. |
| CH-U37 | Complete `system.mutations` catalog | Mutation progress is not a complete SQL-readable history tied to affected parts and errors. | Restart retention, bounded cardinality, and redaction. |
| CH-U38 | Sampled query-log export | Query history/traces are bounded, but there is no configurable probabilistic query-log sampler with aggregate counters. | Stable sampling, literal privacy, and bounded CPU/storage. |
| CH-U39 | Workload admission priorities | Query resource limits do not provide a scheduler with user-defined workload classes and starvation protection. | Fairness, cancellation, and default zero overhead. |
| CH-U40 | Dependency-aware result invalidation | Cached SQL results do not carry a first-class source/partition dependency set for selective invalidation. | No stale result, compact dependency storage, and mutation benchmark. |
| CH-U41 | Schema-versioned plan cache | Prepared query caching does not persist reusable plans across restart with schema dependency validation. | Safe invalidation, parameter isolation, and memory cap. |
| CH-U42 | SQL approximate distinct aggregate | HyperLogLog exists as a data structure, but SQL does not expose an approximate `uniq` aggregate with mergeable state. | Error bounds, NULL/type semantics, and state-wire size. |
| CH-U43 | SQL t-digest quantiles | Quantile sketches exist, but SQL has no mergeable t-digest-style quantile aggregate. | Rank-error contract, retractions, merge exactness, and memory. |
| CH-U44 | SQL aggregate combinators | Aggregate-state helpers are present, but SQL syntax does not expose a general `State`, `Merge`, and filtered combinator family. | Parser compatibility, type checking, and deterministic state serialization. |
| CH-U45 | `WITH TIES` limit semantics | Ordered SQL limits do not expose tie-preserving output around the boundary. | NULL/collation behavior, stable ties, and streaming memory. |
| CH-U46 | `ASOF JOIN` execution | SQL does not provide an ordered nearest-prior temporal join operator. | Timestamp ordering, duplicate ties, NULLs, and index usage. |
| CH-U47 | Dictionary lookup functions | There is no SQL dictionary object/function contract with refresh, miss, and version semantics. | Bounded lookup latency, refresh atomicity, and secret handling. |
| CH-U48 | Ranked full-text search | Token prefilters do not provide exact scored full-text results with BM25-like ranking. | Unicode/tokenization, update cost, and bounded result memory. |
| CH-U49 | Skip-index explain diagnostics | Explain output does not report per-index bytes/rows skipped and false-positive work for a query. | No query-result changes, bounded metrics, and low default overhead. |
| CH-U50 | Part/WAL-consistent backup manifest | Backup and restore are tested, but there is no ClickHouse-style manifest joining immutable parts, checksums, and the exact mutation/WAL boundary. | Concurrent writes, restore rehearsal, crash recovery, and bandwidth. |

## Materialize

| ID | Candidate | Current gap | Adoption gate |
|---|---|---|---|
| M-U01 | Durable connector lifecycle state | `hatPipeline.ConnectorRegistry` now owns create/start/pause/resume/stop transitions and bounded status history, but lifecycle state is in-memory and is not restored across restart. | Crash/restart recovery, version compatibility, bounded persistence, and no query-path overhead. |
| M-U02 | Connector schema evolution | SQL sources do not coordinate upstream add/drop-column changes with dependent objects. | Mixed-version reads, atomic catalog update, and rollback. |
| M-U03 | External source snapshot ingestion | There is no production connector for Kafka/Postgres/CDC snapshot ingestion; callers provide resolver data. | Authentication, offsets, backpressure, and recovery. |
| M-U04 | Multi-source snapshot coordinator | Frontier barriers exist, but no coordinator captures independent source snapshots and their live tails as one initial view. | Blocking, source failure, and exact version retention. |
| M-U05 | Arrangement-only recovery mode | Persisted state can hydrate selected structures, but no public recovery mode restores all maintained arrangements without rereading a source. | Checkpoint compatibility, missing history, and source-version validation. |
| M-U06 | Arbitrary differential window frames | Incremental windows cover bounded append-only cases, not general frames with retractions and late updates. | Exact SQL frame semantics, memory, and late-data behavior. |
| M-U07 | Declarative late-data dataflow policy | Late-data helpers exist, but a maintained object cannot declare allowed lateness, correction, and frontier policy together. | Retraction correctness, retention, and operator reporting. |
| M-U08 | Temporal join state compaction | Temporal joins lack a common policy to evict history after both input frontiers advance. | Out-of-order rows, historical reads, and memory reduction. |
| M-U09 | Durable frontier snapshots | `hatPipeline.FrontierRegistry` now provides deterministic versioned binary `MarshalSnapshot`/`RestoreSnapshot` checkpoints with bounded validation; the caller still owns durable file/WAL ordering and restart orchestration. | Crash/restart recovery, version compatibility, atomic persistence, and no false completion. |
| M-U10 | Adaptive logical compaction | Differential state does not tune compaction from update rate, frontier age, and memory budget. | Exact multiplicity, bounded CPU spikes, and cancellation. |
| M-U11 | Arrangement reuse advisor | Indexes/arrangements are reusable, but no advisor identifies compatible existing arrangements for a new dataflow. | Deterministic recommendations, stale catalog handling, and memory estimates. |
| M-U12 | Arrangement keys in `EXPLAIN` | Explain output does not show arrangement keys, reuse, cardinality, and memory estimates per operator. | Explain-only behavior, bounded output, and no execution. |
| M-U13 | Dataflow cardinality estimates | Plans do not expose estimated versus observed cardinality for filters, joins, and aggregates. | Uncertainty reporting, privacy, and no plan regression. |
| M-U14 | Full object dependency catalog | System tables do not form a complete versioned graph of sources, views, indexes, sinks, and dependencies. | Bounded traversal, cycle handling, and schema stability. |
| M-U15 | SQL source-status catalog | Source metrics exist, but lifecycle/lag/error status is not queryable as a stable SQL system relation. | Label cardinality, redaction, and snapshot consistency. |
| M-U16 | Transactional view create/replace | DDL cannot atomically replace a maintained object and its dependents with a catalog version boundary. | Rollback, concurrent readers, and dependency validation. |
| M-U17 | Backfill/frontier handoff | A new view/index cannot be backfilled from a snapshot and atomically join the live update stream at one frontier. | No duplicate/missing updates, cancellation, and cutover atomicity. |
| M-U18 | Exactly-once sink checkpoints | Sink progress exists, but external delivery lacks a durable idempotency key and acknowledged frontier contract. | Crash/retry behavior, duplicate suppression, and bounded history. |
| M-U19 | Source transaction grouping across relations | Source transaction grouping is specialized; there is no general cross-relation transaction envelope for all source adapters. | Atomic visibility, malformed input, and recovery. |
| M-U20 | Secret/connection resources | Authentication exists, but SQL has no named redacted connection and secret objects with scoped ownership and rotation. | Redaction, reload, authorization, and no credential leakage. |
| M-U21 | Role and namespace hierarchy | Namespace concepts exist, but there is no complete SQL catalog of roles, grants, ownership, and inheritance. | Default deny, migration compatibility, and policy invalidation. |
| M-U22 | Connector transaction retry journal | Source offsets do not retain connector transaction intent and retry outcome as a durable bounded record. | Idempotent replay, ambiguous failure handling, and retention. |
| M-U23 | Per-cluster query admission | Query cancellation and quotas exist, but no scheduler reserves CPU/memory pools per cluster for serving and maintenance. | Fairness, starvation prevention, and cancellation. |
| M-U24 | Workload classes and priorities | Sources, computes, sinks, and ad-hoc queries cannot declare a shared priority class. | Backward-compatible defaults and starvation tests. |
| M-U25 | Durable redacted query history | Query history is bounded, but there is no restart-persistent redacted audit stream with retention controls. | Literal/secret redaction, quotas, and disk failures. |
| M-U26 | Unified source/compute/sink metrics catalog | Monitoring metrics are not one SQL-readable object model tied to dataflow objects. | Cardinality bounds, scrape overhead, and stable schema. |
| M-U27 | Logical publication/subscription | Replication transfers cache state, not a versioned maintained SQL change publication with consumer checkpoints. | Schema evolution, exactly-once boundaries, and backpressure. |
| M-U28 | General monotonicity inference | Monotonicity helpers cover selected typed-table cases, not planner-wide joins, filters, aggregates, and envelopes. | Conservative fallback, NULLs, retractions, and proof tests. |
| M-U29 | General recursive dataflow maintenance | Recursive maintenance is append-only/opt-in, not a complete retraction-capable recursive dataflow engine. | Cycles, termination, retractions, and memory bounds. |
| M-U30 | Arrangement-native top-N retractions | Differential grouped top-N exists, but generic maintained ordered arrangements do not update top-N without rebuilding incompatible state. | Duplicate ties, deletes, and update cost. |
| M-U31 | User-defined retractable aggregates | Aggregate functions cannot declare a merge/retract state contract for incremental maintenance. | Type safety, rollback, serialization, and panic isolation. |
| M-U32 | UDF purity classification | Custom SQL functions are not cataloged as deterministic, monotone, or retractable for safe incremental plans. | Conservative planning, error behavior, and authorization. |
| M-U33 | As-of retention policy | `hatPipeline.FrontierRetentionRegistry` now tracks bounded per-frontier as-of leases and exposes the safe compaction boundary; storage engines must still consult it and recreate leases after restart. | No premature compaction, bounded storage, and removal. |
| M-U34 | Historical subscription cancellation | Durable subscriptions lack a complete cancellation/checkpoint protocol for historical replay interrupted mid-stream. | No duplicate/missing records and restart recovery. |
| M-U35 | Snapshot blocking semantics | Source snapshotting does not expose a common queryable state that blocks dependents until all required objects are ready. | Per-object progress, cancellation, and read consistency. |
| M-U36 | Hydration progress and admission | Arrangement hydration lacks a first-class progress/status contract that controls query admission. | Restart latency, readiness, and no source reread. |
| M-U37 | Compaction diagnostics by arrangement | Compaction metrics are broad, not a stable per-arrangement history of logical/physical bytes and compaction debt. | Bounded labels, low overhead, and accurate accounting. |
| M-U38 | Persisted immutable data parts | Persistence uses existing storage paths, but there is no Materialize-style immutable part manifest with object-store lifecycle. | Checksums, garbage collection, crash recovery, and read amplification. |
| M-U39 | Partition/order declarations | SQL objects cannot declare expected partition ordering that is exposed to planning and filter pushdown. | No semantic data rewrite, explain visibility, and fallback. |
| M-U40 | Source schema registry integration | CDC/schema changes are not validated against a registry version and compatibility policy. | Version mismatch, rollback, and secret-safe diagnostics. |
| M-U41 | Webhook source idempotency | External event ingestion lacks a durable event-ID deduplication contract. | Replay safety, expiry, and bounded state. |
| M-U42 | Connector pause/resume checkpoints | A source cannot be paused and resumed with a durable offset/frontier checkpoint through one public operation. | Crash recovery, stale resumes, and operator audit. |
| M-U43 | Sink backpressure catalog | Sink lag/backpressure is not exposed as a common frontier-aware state that can throttle upstream work. | No deadlock, cancellation, and bounded queues. |
| M-U44 | Cross-dataflow transaction visibility | Independent maintained views do not share a transaction-wide logical timestamp for atomic multi-object observations. | Consistent reads, failure recovery, and latency. |
| M-U45 | Incremental join index selection | Join plans do not automatically choose and maintain the smallest compatible arrangement for changing predicates. | Duplicate semantics, update cost, and explainability. |
| M-U46 | Differential export/import | There is no portable checkpoint format for exporting signed differential updates and importing them at a frontier. | Versioning, type preservation, and atomic import. |
| M-U47 | Progress-only subscription frames | Subscription streams do not have a compact standard frame for frontier advancement independent of row updates across every API. | Ordering, cancellation, and wire compatibility. |
| M-U48 | Source connector health remediation | Health status is observable, but no policy can pause, retry, or quarantine a failed connector with bounded backoff. | Avoid retry storms, preserve offsets, and operator override. |
| M-U49 | Catalog migration runner | SQL object changes lack a dependency-aware dry-run/apply/rollback migration plan. | Partial failure recovery, lock scope, and mixed-version clients. |
| M-U50 | Durable frontier-based backup | Backup does not capture all source offsets, arrangement frontiers, and subscription checkpoints as one recoverable logical snapshot. | Restore ordering, missing history, and rehearsal verification. |

## Tarantool

| ID | Candidate | Current gap | Adoption gate |
|---|---|---|---|
| T-U52 | Per-peer adaptive breaker policy | `hatPeer.ConnectionPool` now supports opt-in bounded cooldown backoff after failed probes and decay after recovery; failure-class-specific thresholds remain caller policy. | Preserve deterministic operator bounds, avoid false opens, keep the default disabled, and validate recovery behavior. |
| T-U02 | Authenticated compact peer daemon integration | `hatPeer` now provides an explicit bounded listener with fixed-size version/feature negotiation, mandatory authorization, optional TLS enforcement, admission limits, handshake deadlines, and clean session shutdown; full cluster membership remains caller-owned. | Complete compatibility evolution, flow-control policy, and head-of-line behavior without enabling a daemon by default. |
| T-U03 | Stored procedure registry | External extension boundaries exist, but no trusted in-process stored function registry exposes stable call semantics. | Authorization, panic isolation, and versioning. |
| T-U04 | Sandboxed stored Lua/runtime functions | There is no resource-limited embedded scripting runtime for stored procedures. | Sandbox escape resistance, CPU/memory limits, and disable-by-default policy. |
| T-U05 | Session transaction settings | Transactions exist, but client/session defaults for isolation, timeout, read-only, and durability are not a unified contract. | Inheritance, reset, and authorization. |
| T-U06 | Replica-wide read-only enforcement | Leader-write enforcement protects selected service paths, but every direct mutation API cannot be locked by a replica read-only state. | No bypass path, internal replication exception, and operator override. |
| T-U07 | Replica lag/RPO status object | `hatReplication` now provides clock-independent underflow-safe per-replica RPO status values and a bounded order-preserving batch builder; integration with live node metrics and failover policy remains caller-owned. | Bounded metrics, clock safety, and failover use. |
| T-U08 | Snapshot plus exact WAL coordinate | `hatCache.CommandJournal` now offers an opt-in online snapshot manifest with exact journal sequence, byte count, SHA-256, and a file verifier; snapshot-plus-WAL join orchestration remains caller-owned. | Concurrent writes, checksums, and restore rehearsal. |
| T-U09 | Snapshot-plus-WAL join bootstrap | Cluster join lacks a standard snapshot transfer, journal catch-up, fencing, and atomic activation workflow. | Duplicate apply, failure cleanup, and stale member fencing. |
| T-U10 | Journal-wide synchronous write quorum | Quorum APIs are selective; no per-write journal contract waits for configured replica acknowledgements. | Unavailable peers, latency, retry ordering, and default off. |
| T-U11 | Per-space conflict policy | `hatReplication.ConflictPolicyRegistry` provides bounded per-space last-write-wins, source-priority, and reject policies with copied configuration and deterministic fallbacks; applying it to a concrete write path remains caller-owned. | Deterministic clocks, auditability, and migration. |
| T-U12 | Automatic failover workflow | Election/fencing primitives exist, but health-triggered promotion with quorum and operator policy is not complete. | Split-brain prevention, false positives, and override. |
| T-U13 | Durable cluster membership | Topology membership is not a durable consensus-backed join/leave record with generations. | Stale membership, backups, and quorum loss. |
| T-U14 | VShard bucket map and migration | Partitioning exists, but automatic bucket ownership and migration with backup semantics are not implemented. | Ownership, migration interruption, and recovery design. |
| T-U15 | Named space catalog | `hatSchema.SpaceCatalog` combines a named versioned source, constraints, and validated hash/tree/R-tree/functional index declarations with bounded clone-safe lookup and listing; execution-layer index construction remains caller-owned. | Field numbering, schema validation, and evolution. |
| T-U16 | Selectable memtx-style row engine | HAT-trie and typed layouts exist, but no per-space predictable in-memory tuple engine is selectable. | Memory overhead, tuple access, and benchmark crossover. |
| T-U17 | Selectable vinyl-style LSM engine | Pebble persistence exists, but no user-selectable table engine exposes explicit LSM read/write tradeoffs. | Compaction, read amplification, backup, and recovery. |
| T-U18 | Explicit volatile cache engine | TTL/cache commands exist, but durability intent is not represented by a separate memory-only engine contract. | Eviction, memory accounting, and operator safety. |
| T-U19 | Durable tuple field-operation journal | Allocation-light generic tuple assignment/add/splice batches exist, but those field operations are not represented by a durable replayable journal record. | Overflow/type recovery, atomic replay, version compatibility, and bounded record size. |
| T-U20 | Online space upgrade | Schema validation exists, but records cannot be converted in the background while compatible reads/writes continue. | Dual-format reads, progress, cutover, and crash recovery. |
| T-U21 | Versioned space migration manager | There is no durable named migration plan with preconditions, progress, mixed-version clients, and rollback. | Resume after crash and dependency validation. |
| T-U22 | Cross-index unique constraints | Hash/ordered/functional indexes exist, but a named space cannot atomically enforce uniqueness across multiple maintained indexes. | Concurrent writes, rollback, and error determinism. |
| T-U23 | Automatic multikey tuple indexes | Array/multikey helpers exist, but generic tuple nested-array expansion is not a maintained space-index contract. | Duplicate elements, delete correctness, and explosion limits. |
| T-U24 | Conditional space indexes | Conditional functional indexes exist, but there is no schema-level conditional index lifecycle with planner metadata. | Predicate determinism, updates, rebuild, and false negatives. |
| T-U25 | R-tree space integration | An R-tree exists, but tuple writes and SQL planning do not automatically maintain/use it as a named-space index. | Update/delete correctness and rebuild cost. |
| T-U26 | Index hints and strategy inspection | Callers cannot request a specific embedded index strategy or inspect why another was selected. | Unsupported-hint errors and explainability. |
| T-U27 | Prefix/config watchers over peer connections | Local key watchers exist, but peer connections cannot watch remote configuration paths/prefixes with reconnect semantics. | Ordering, reconnect gaps, auth, and backpressure. |
| T-U28 | Connection/schema lifecycle triggers | `hatPeer.PeerLifecycleRegistry` now provides bounded connect, disconnect, shutdown, and schema-reload hooks, and `CompactPeerSession` emits opt-in connection terminal events; other daemon, pool, and schema paths still do not publish lifecycle events. | Callback ordering, concurrency, cleanup, and broader integration. |
| T-U29 | Stream transaction API | `hatPeer.CompactPeerStreamEndpoint` now provides bounded begin/call/commit/rollback streams over one compact connection, with ordered per-stream calls and server-side state validation; durable transaction recovery remains handler-owned. | Cancellation, connection loss, transaction recovery, and atomicity. |
| T-U30 | Cooperative fiber scheduler | Pipelines and goroutines exist, but no fiber abstraction provides yield/resume scheduling with measured stack overhead. | Fairness, cancellation, and comparison against goroutines. |
| T-U31 | Fiber channels and conditions | General typed coordination exists, not fiber-aware channels, conditions, semaphores, and wait groups. | Close semantics, deadlock behavior, and memory. |
| T-U32 | Fiber-local storage | Request context exists, but no safe task-local storage contract avoids accidental value leakage across reused workers. | Cleanup, reuse, and race coverage. |
| T-U33 | Function grants and roles | Service authorization exists, but function/space-level grants are not a complete embedded least-privilege model. | Default deny, policy cache invalidation, and audit. |
| T-U34 | Per-space WAL sync policy | Journal group commit exists, but a named space cannot choose synchronous, periodic, or disabled WAL behavior. | Unsafe-default prevention and crash tests. |
| T-U35 | WAL encryption and key rotation | Binary journal/storage codecs exist, but there is no authenticated encrypted WAL envelope with rotation. | Nonces, corruption recovery, rotation, and secret handling. |
| T-U36 | Snapshot rotation policy | Backups exist, but no engine policy coordinates snapshot cadence, WAL replay, disk budget, and retention. | Crash consistency and predictable recovery time. |
| T-U37 | Replica applier throttling | Replication does not expose a bounded policy to throttle or prioritize apply work against foreground traffic. | No starvation, lag bounds, and tail latency. |
| T-U38 | Conflict introspection stream | Conflict resolution does not expose a durable redacted stream of conflicted keys, sources, and decisions. | Privacy, bounded retention, and replay diagnostics. |
| T-U39 | Space-level changefeed | Key watchers and SQL subscriptions exist, but no named-space changefeed has schema/version, checkpoint, and backpressure semantics. | Ordering, retractions, reconnect, and checkpoint recovery. |
| T-U40 | Tuple format version negotiation | `hatDataStructure.TupleFormat` now exposes bounded capability advertisements with stable physical-shape fingerprints, deterministic HTF1 encoding, and highest exact common-version selection; the caller still owns the transport handshake and migration policy. | Unknown-field behavior and downgrade safety. |
| T-U41 | Snapshot-consistent iterator cursors | `hatDataStructure.OrderedIndex.SnapshotCursor` now provides an opt-in zero-copy stable view across concurrent mutations, with explicit seek/close/EOF semantics and existing copy-on-write retention; named-space registry wiring remains caller-owned. | Memory bound, invalidation, and repeatable ordering. |
| T-U42 | Cursor `after` pagination contract | `hatDataStructure.CursorTokenCodec` now provides bounded HMAC-authenticated index/schema-bound continuation tokens, and ordered cursors resume after the complete (key, ID) position; transport and SQL endpoint integration remain caller-owned. | Schema/version binding, tamper resistance, and no skipped rows. |
| T-U43 | Per-space memory quotas | `hatStorage.SpaceMemoryQuota` and its registry now provide opt-in named-space byte admission with atomic reserve/release, deterministic snapshots, and default zero integration overhead; callers still declare logical bytes and release them on free. | Accurate attribution, no deadlock, and default zero overhead. |
| T-U44 | Slab fragmentation diagnostics | Runtime heap metrics do not expose allocator classes, fragmentation, and reusable free space in a portable read-only report. | Platform portability and sampling cost. |
| T-U45 | Per-space operation statistics | `hatMetrics.SpaceOperationMetrics` and its bounded registry now provide opt-in named space/index counters for operation families, bytes, hits/misses/errors, and latency totals with an allocation-free handle path; callers still choose integration points and label cardinality. | Label cardinality and allocation-free default path. |
| T-U46 | Index cardinality/hot-key statistics | `hatDataStructure.IndexStats` now provides opt-in fixed-memory HyperLogLog cardinality, exact posting-length counters, and Space-Saving hot-key hashes; index structures remain uninstrumented until callers observe keys/lookups. | Privacy, sampling, and update overhead. |
| T-U47 | Cancellation/deadline propagation to peer calls | `hatPeer.ConnectionPool.DoWithLifecycleContext` now composes caller cancellation/deadlines with pool shutdown for opt-in gRPC, HTTP/2, and compact-protocol handlers; legacy `Do` remains unchanged for zero-allocation callers. | Connection reuse, partial response, and leak tests. |
| T-U48 | Idempotent remote-call retry policy | `hatPeer.RetryPolicy` now provides opt-in method-aware bounded retries with stable idempotency keys, fencing tokens, cancellation-aware exponential backoff/jitter, and observer events; remote handlers still enforce deduplication. | No duplicate mutation, jitter, and observability. |
| T-U49 | Replica-set request hedging | `hatTopology.ExecuteReplicaHedged` now provides opt-in bounded read hedging with delayed fallback, first-success cancellation, deterministic failures, observer events, and a zero-allocation single-candidate fast path. | Tail-latency versus duplicate load and consistency. |
| T-U50 | Cluster-wide configuration watch | `hatTopology.ConfigWatchLog` now provides an authenticated bounded versioned log with replay cursors, context-aware wait/resume, deterministic history-gap errors, value-copy isolation, and no per-client idle goroutine; transport and consensus remain caller-owned. | Gap recovery, authorization, and bounded history. |

## Selection Policy

The next implementation should be chosen from a row whose public integration
boundary is clear and whose default behavior can remain unchanged. Storage,
replication, backup, and SQL planner changes require broader correctness tests;
generic `hat*` primitives are preferred when they provide a real reusable
contract rather than an unintegrated placeholder. Large or irreversible ideas
such as VShard migration, a new scripting runtime, or a consensus log remain
proposal-only until their recovery and security contracts are complete.
