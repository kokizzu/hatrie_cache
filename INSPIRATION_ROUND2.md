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

- [ ] C201 Adaptive asynchronous-insert flush timeout based on arrival rate.
- [ ] C202 Per-shard asynchronous-insert buffer affinity to reduce cross-shard coordination.
- [ ] C203 Explicit `wait_for_async_insert` durability modes with visible acknowledgment semantics.
- [ ] C204 Idempotency-token propagation across asynchronous inserts and dependent materialized views.
- [ ] C205 Query-cache controls scoped to individual subqueries.
- [ ] C206 Query-cache eligibility checks that reject nondeterministic expressions.
- [ ] C207 Query-condition cache with data-generation invalidation for repeated filters.
- [ ] C208 Query-cache hit, miss, bypass, and eviction metrics.
- [ ] C209 Automatic basic column statistics for row count, null count, min, and max.
- [ ] C210 Compact histograms for cardinality and selectivity estimates.
- [ ] C211 Statistics-driven join-order selection with deterministic fallback.
- [ ] C212 Precomputed hash and software-prefetch hooks for hot hash-table probes.
- [ ] C213 Cached JIT expression plans with bounded compilation memory.
- [x] C214 Resource-bounded WebAssembly UDF execution; see [C214_BOUNDED_WASM_UDF.md](C214_BOUNDED_WASM_UDF.md).
- [ ] C215 External dictionary reloads with TTL and last-known-good retention.
- [ ] C216 Dictionary layout selection based on key cardinality and lookup shape.
- [ ] C217 Time-series `WITH FILL` gap generation over ordered results.
- [ ] C218 Interpolation policies for filled time-series values.
- [ ] C219 Per-group `LIMIT BY` execution with bounded memory.
- [x] C220 Post-window `QUALIFY` filtering over projected window rows; selected window aliases are evaluated before `DISTINCT`, `ORDER BY`, and `LIMIT`; see [C220_QUALIFY.md](C220_QUALIFY.md).
- [ ] C221 `WITH TIES` limit semantics for deterministic boundary results.
- [ ] C222 Approximate top-K aggregation with mergeable bounded state.
- [ ] C223 Mergeable approximate distinct and quantile aggregate states.
- [ ] C224 `argMax` and `argMin` aggregate states with deterministic tie handling.
- [ ] C225 Incremental window-frame state for repeated ordered windows.
- [ ] C226 Grace-hash join spilling with bounded disk runs.
- [ ] C227 External aggregation spilling with merge-time memory limits.
- [ ] C228 External sort spilling with stable run ordering.
- [ ] C229 Join overflow policies that explicitly reject, spill, or truncate.
- [ ] C230 Memory-overcommit wait queues before query cancellation.
- [ ] C231 Workload groups with per-class concurrency and memory budgets.
- [ ] C232 Query-complexity limits for rows, joins, and intermediate results.
- [ ] C233 Per-query CPU-time budgets with cooperative cancellation.
- [ ] C234 Query profiler records for stage time, bytes, and allocations.
- [ ] C235 Read/write task profiler aggregation by table part and column.
- [ ] C236 Explain output for data-skipping-index decisions and rejected marks.
- [ ] C237 Explain output for projection selection and estimated I/O cost.
- [ ] C238 Mutation queue progress with rows remaining and elapsed estimates.
- [ ] C239 Part-merge backlog, amplification, and age metrics.
- [ ] C240 Read-only backup database attachment for querying backup parts in place.
- [ ] C241 Incremental backup chunk deduplication across snapshots.
- [ ] C242 Parallel restore of independent parts with bounded concurrency.
- [ ] C243 Remote-part read-through caching with immutable checksum keys.
- [ ] C244 Local cache reuse validated by part and column checksums.
- [ ] C245 Vertical TTL deletion that reads only the deletion mask and key columns.
- [ ] C246 TTL-driven recompression policies separate from row deletion.
- [ ] C247 Codec selection for delta, Gorilla, and ALP-style numeric compression.
- [ ] C248 Variant/JSON subcolumn projection that reads only referenced paths.
- [ ] C249 Kafka-style offset inspection without committing consumer position.
- [ ] C250 Retry-safe insert identities shared across asynchronous ingestion stages.

## Materialize: 50 Additional Ideas

- [x] M201 Changefeed progress messages that certify a timestamp frontier. `hatReplication.ChangefeedFrontier` publishes allocation-free monotonic sequence progress with idempotent equal advances and regression rejection; see [CHANGEFEED_PROGRESS.md](CHANGEFEED_PROGRESS.md).
- [x] M202 Durable subscription resume from a persisted `AS OF` frontier. `hatReplication.ChangefeedCheckpoint` provides a strict source-bound binary checkpoint that advances only from valid monotonic progress; see [CHANGEFEED_CHECKPOINT.md](CHANGEFEED_CHECKPOINT.md).
- [ ] M203 Snapshot-free subscription mode for consumers that already have state.
- [ ] M204 Bounded subscriptions with an exclusive `UP TO` timestamp.
- [x] M205 Deterministic within-timestamp ordering for changefeed batches. Opt-in `QuerySubscriptionDefinition.DeterministicOrder` sorts differential initial, update, progress-safe, and reset batch phases by canonical row key without changing default behavior; see [M205_DETERMINISTIC_SUBSCRIPTION_ORDER.md](M205_DETERMINISTIC_SUBSCRIPTION_ORDER.md).
- [ ] M206 Upsert envelopes that expose a stable key and current row image.
- [x] M207 Debezium envelopes with before/after images and operation type. `hatSql.DebeziumChangefeed` requires declared unique key columns, emits snapshot/create/update/delete payloads, preserves subscription frontier metadata, and rejects ambiguous multiplicity; the optimized adapter measured about 16x lower latency, 18x lower allocated bytes, and 10x fewer allocations than its full-state-copy baseline; see [M207_DEBEZIUM_CHANGEFEED.md](M207_DEBEZIUM_CHANGEFEED.md).
- [ ] M208 Differential multiplicity folding for insert/delete update streams.
- [ ] M209 Monotone logical timestamp frontiers for read and stream APIs.
- [ ] M210 Historical `AS OF` reads against retained logical state.
- [ ] M211 Explicit rejection of reads before `since` or at/after `upper` frontiers.
- [ ] M212 Logical compaction that advances retained history without rewriting live state.
- [ ] M213 Consolidation of equal updates before forwarding to downstream consumers.
- [ ] M214 Arrangement reuse across indexes and compatible query plans.
- [ ] M215 Delta-join maintenance for high-churn join inputs.
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
- [ ] M249 Consistency fencing between a point read and a subsequent subscription.
- [ ] M250 Temporal join alignment that waits for both input frontiers.

## Tarantool: 50 Additional Ideas

- [ ] T201 Per-space synchronous replication quorum for critical records only.
- [ ] T202 Automatic leader election for a replica set.
- [ ] T203 Strict leader fencing against stale writers after failover.
- [ ] T204 Supervised failover with explicit operator override and recovery state.
- [ ] T205 LSN-based replication lag and apply-throughput metrics.
- [ ] T206 Deterministic replica bootstrap and join workflow.
- [ ] T207 Replica eviction, rejoin, and stale-state recovery protocol.
- [ ] T208 Anonymous replicas that do not participate in quorum decisions.
- [ ] T209 Relay/applier backpressure when a replica falls behind.
- [ ] T210 Master-master conflict hooks with source and sequence context.
- [ ] T211 Configurable WAL synchronization modes with durability reporting.
- [ ] T212 WAL retention and rotation policies tied to replica acknowledgments.
- [ ] T213 Scheduled snapshots with checkpoint manifests and atomic publication.
- [ ] T214 Streaming snapshots for replicas without a shared filesystem.
- [ ] T215 Per-space memtx versus on-disk storage policy.
- [ ] T216 Vinyl-style run compaction scheduling and space accounting.
- [ ] T217 In-memory columnar storage for analytical spaces.
- [ ] T218 Multi-part TREE indexes with ordered prefix and range scans.
- [ ] T219 HASH indexes for constant-time exact lookups.
- [ ] T220 RTREE indexes for spatial bounding-box searches.
- [ ] T221 BITSET indexes for low-cardinality membership predicates.
- [ ] T222 Multikey indexes over array-valued fields.
- [ ] T223 Functional indexes over derived field expressions.
- [ ] T224 Partial indexes restricted by a validated predicate.
- [ ] T225 Covering indexes that return projected fields without row fetches.
- [ ] T226 Explicit index hints with planner diagnostics.
- [ ] T227 Per-field nullability, type, and constraint validation.
- [ ] T228 Tuple-format schema versions with compatible readers.
- [ ] T229 Before-replace triggers for validation and conflict policy.
- [ ] T230 On-replace changefeed hooks with old and new tuple images.
- [ ] T231 After-replace audit hooks with transaction identity.
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
