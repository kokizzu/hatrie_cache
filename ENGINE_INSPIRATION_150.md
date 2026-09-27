# Engine Inspiration Gap Register

This register contains 50 ClickHouse, 50 Materialize, and 50 Tarantool ideas
that are not currently complete user-facing capabilities in `hatrie_cache` as
of commit `4349ea1c`. Existing adjacent primitives do not count as adoption;
each row names the missing end state that would need its own test and benchmark.

The implementation rule for every row is: add a focused failing test, capture a
baseline, implement the smallest compatible slice, rerun correctness and race
tests, benchmark CPU/allocations/memory/bytes as applicable, and keep or revert
the change based on measured value and operational tradeoffs.

Primary references used for the gap scan:

- ClickHouse: <https://clickhouse.com/docs/>
- Materialize arrangements: <https://materialize.com/docs/fundamentals/concepts/arrangements/>
- Materialize subscriptions: <https://materialize.com/docs/sql/subscribe/>
- Materialize durable subscriptions: <https://materialize.com/docs/serve-results/durable-subscriptions/>
- Tarantool platform reference: <https://www.tarantool.io/en/doc/latest/singlepage/>

## ClickHouse: 50 Gaps

| ID | Missing capability | Adoption shape | Measure and rollback gate |
| --- | --- | --- | --- |
| CHG-001 | Automatic `PREWHERE` split for selective predicates | Separate cheap/filter columns before wide projection materialization | Compare bytes decoded, CPU, and result equivalence; revert if narrow scans regress |
| CHG-002 | Query-aware `PREWHERE` column dependency planning | Derive the smallest early column set from the predicate AST | Measure decoded column bytes and planning time |
| CHG-003 | Projection freshness and invalidation state machine | Track source versions, failed refreshes, and stale-read policy per projection | Measure refresh lag and stale-result incidents; retain explicit fallback |
| CHG-004 | Cost-based projection rewrite in the normal planner | Select a compatible projection without requiring an explicit hint | Compare plan latency and query CPU against the base scan |
| CHG-005 | Expression-based data-skipping index definitions | Allow reusable predicates beyond the current JSON-specific indexes | Measure index build bytes and false-negative safety with differential tests |
| CHG-006 | Granule Bloom filter sidecar for string equality | Store bounded Bloom summaries beside row groups | Measure false-positive rate, retained bytes, and scan reduction |
| CHG-007 | Token Bloom filter (`tokenbf`) for word searches | Add configurable token hashing and per-granule summaries | Measure lookup CPU, false positives, and memory per row |
| CHG-008 | N-gram Bloom filter for wildcard search | Accelerate anchored and internal `LIKE` literals with safe n-grams | Compare candidate granules and worst-case fallback cost |
| CHG-009 | Bounded `set` skipping index for low-cardinality granules | Store exact distinct values up to a configured cap | Measure cap overflow behavior and bytes per granule |
| CHG-010 | Multi-column min/max mark pruning | Use lexicographic and independent bounds for compound predicates | Differentially compare selected marks with a full scan |
| CHG-011 | Primary-key mark cache | Cache mark ranges by source version and normalized predicate | Measure hit rate, retained memory, and invalidation cost |
| CHG-012 | Mark-cache admission by reuse probability | Admit marks only after repeated access or bounded workload evidence | Compare cache hit rate and memory against eager admission |
| CHG-013 | Granule-level query condition cache | Cache boolean granule decisions keyed by source version | Verify invalidation and measure CPU/bytes saved |
| CHG-014 | Adaptive remote-part prefetch | Prefetch the next predicted part or mark range asynchronously | Measure tail latency, extra bandwidth, and cancellation waste |
| CHG-015 | Remote cache promotion by access frequency | Promote hot remote objects while suppressing one-hit retention | Compare hit rate, disk growth, and read latency |
| CHG-016 | Background merge selection by write amplification | Prefer merges that reduce future read and storage amplification | Measure merge CPU, write bytes, and query impact |
| CHG-017 | Fair merge scheduling across partitions | Prevent a hot partition from starving cold partitions | Measure queue age distribution and throughput |
| CHG-018 | Vertical merge for wide rows | Merge key columns separately from cold payload columns | Measure peak memory and write amplification |
| CHG-019 | Small-part compacting insert path | Coalesce tiny inserts before normal part creation | Compare insert latency, part count, and recovery behavior |
| CHG-020 | Merge cancellation and resumable checkpoints | Stop low-value merges under pressure and resume safely | Measure wasted work and recovery correctness |
| CHG-021 | Async-insert busy timeout and backpressure | Flush by size or bounded wait while exposing queue pressure | Compare p99 insert latency and batch size |
| CHG-022 | Async-insert grouping by schema and destination | Avoid mixed-batch conversion and preserve per-source ordering | Measure allocations and flush throughput |
| CHG-023 | Mutation dependency graph wired to SQL DML | Turn `ALTER`/`DELETE` dependencies into executable ordered tasks | Test crash recovery and measure mutation wait time |
| CHG-024 | Lightweight delete bitmap auto-discovery | Discover part manifests and attach tombstones without caller wiring | Compare delete latency and read filtering cost |
| CHG-025 | Lazy materialized/default-column backfill | Compute old rows on read until a bounded background backfill completes | Measure read CPU and migration downtime |
| CHG-026 | TTL move-to-tier policy | Move expired or cold data to a cheaper tier before deletion | Measure storage cost, read latency, and recovery paths |
| CHG-027 | TTL recompression policy | Recompress cold parts with a lower-cost codec | Compare CPU, bytes, and read amplification |
| CHG-028 | TTL scheduler jitter and fairness | Spread maintenance work to avoid synchronized spikes | Measure maintenance CPU peak and deadline lag |
| CHG-029 | Refreshable materialized-view dependency runner | Refresh views after source version changes with bounded retries | Measure freshness, retry backlog, and duplicate work |
| CHG-030 | Materialized-view dead-letter and replay | Persist failed view batches for operator-directed replay | Verify idempotency and measure retained failure bytes |
| CHG-031 | Dictionary refresh jitter and stale policy | Stagger refreshes and expose bounded stale reads | Measure refresh bursts and lookup latency |
| CHG-032 | Dictionary lookup result cache invalidation | Invalidate cached lookups by dictionary snapshot version | Compare hit rate and stale-result risk |
| CHG-033 | Normalized query-cache keys | Canonicalize whitespace, literals, settings, and source versions | Measure hit rate and key-build overhead |
| CHG-034 | Result-cache admission by result size and reuse | Reject large or one-shot results from the cache | Compare retained memory and hit-value ratio |
| CHG-035 | Per-query memory budget and spill trigger | Enforce bounded memory with deterministic spill or rejection | Measure peak RSS, spill bytes, and query latency |
| CHG-036 | Workload scheduling by priority and fairness | Queue expensive queries with bounded priority aging | Measure p95/p99 latency and starvation |
| CHG-037 | Automatic two-level aggregation threshold | Select hash layout from estimated group cardinality | Compare CPU, allocations, and retained state |
| CHG-038 | External aggregation spill merge | Spill partial aggregate states and merge them incrementally | Measure peak memory and total I/O |
| CHG-039 | Exact aggregate-state serialization and merge | Persist typed partial states for distributed or resumable work | Differentially compare merged results and wire bytes |
| CHG-040 | Incremental window-frame maintenance | Maintain bounded `ROWS`/`RANGE` frames instead of rescanning partitions | Measure update CPU and state bytes |
| CHG-041 | Streaming `ARRAY JOIN` expansion | Emit expanded rows without materializing the full expansion | Compare peak memory, ordering, and cancellation |
| CHG-042 | Automatic JSON subcolumn materialization | Promote frequently read JSON paths into typed subcolumns | Measure path lookup CPU, bytes, and schema churn |
| CHG-043 | JSON dynamic-type promotion and quarantine | Handle type changes with bounded fallback rather than global failure | Verify mixed-type semantics and migration cost |
| CHG-044 | Parallel-replica range assignment | Split mark ranges among replicas with deterministic reconciliation | Compare wall time, network bytes, and duplicate work |
| CHG-045 | Distributed merge backpressure | Bound result buffering between remote workers and coordinator | Measure memory and tail latency under slow consumers |
| CHG-046 | Incremental backup retention garbage collection | Delete unreachable content-addressed objects after manifest retention | Verify restore chains before deletion and measure disk reclaimed |
| CHG-047 | Encrypted backup envelopes with key rotation | Encrypt payloads and rotate keys without rewriting unchanged objects | Measure CPU, bytes, and restore compatibility |
| CHG-048 | Named settings collections with inheritance | Validate and apply reusable profiles at session/query scope | Measure lookup allocations and cycle rejection |
| CHG-049 | Query-plan diagnostics for skipped marks and indexes | Expose selected/rejected granules and index reasons in `EXPLAIN` | Verify counters match execution and bound report size |
| CHG-050 | Part health and repair prioritization | Rank checksum, missing-part, and replica repair work | Measure repair convergence and foreground impact |

## Materialize: 50 Gaps

| ID | Missing capability | Adoption shape | Measure and rollback gate |
| --- | --- | --- | --- |
| MZG-001 | Automatic plan-wide arrangement reuse | Reuse compatible arrangements across one compiled query graph | Compare state bytes, build CPU, and lookup latency |
| MZG-002 | Cross-alias arrangement canonicalization | Share arrangements when SQL aliases differ but keys are equivalent | Differentially verify key ordering and memory savings |
| MZG-003 | Workload-driven arrangement key selection | Add indexes from observed lookup predicates under a bounded budget | Measure hit rate, maintenance cost, and memory cap |
| MZG-004 | Adaptive logical compaction windows | Tune retention from read-hold age and update rate | Compare retained history, reader pauses, and update CPU |
| MZG-005 | Durable arrangement hydration checkpoints | Persist restartable progress for large state hydration | Measure restart time and checkpoint overhead |
| MZG-006 | Parallel arrangement hydration by partition | Read independent persisted ranges concurrently | Compare wall time, peak memory, and disk I/O |
| MZG-007 | Hydration resume after cancellation | Persist completed ranges and resume without replaying them | Verify snapshot equivalence and saved work |
| MZG-008 | Durable `SUBSCRIBE AS OF` cursor protocol | Persist progress timestamps with output batches | Test crash/restart boundaries and duplicate handling |
| MZG-009 | Progress heartbeats for subscriptions | Emit explicit frontier progress records | Measure consumer lag visibility and wire overhead |
| MZG-010 | Snapshot-free subscription backfill optimization | Skip historical fetch when the requested arrangement proves it unnecessary | Compare startup latency and correctness at `AS OF` |
| MZG-011 | Upsert-envelope differential output | Collapse row retractions into keyed upsert events | Compare bandwidth and duplicate semantics |
| MZG-012 | Debezium-envelope decode and encode | Preserve source transaction/order metadata through changes | Differentially test tombstones and schema evolution |
| MZG-013 | Source offset and output progress coupling | Commit source offsets only after durable output progress | Measure replay volume and failure recovery |
| MZG-014 | Exactly-once sink commit protocol | Couple output batches to an idempotent external commit token | Test crash windows and throughput cost |
| MZG-015 | Sink retry queue with dead-letter state | Retry transient failures without blocking unrelated outputs | Measure backlog age and duplicate prevention |
| MZG-016 | Multi-source frontier coordinator | Compute a safe joint frontier for joins and unions | Compare wait time and stale-read rejection |
| MZG-017 | Timestamp oracle with monotonic fencing | Allocate ordered logical timestamps across writers | Measure allocation latency and failover behavior |
| MZG-018 | Timestamp lease expiry and writer fencing | Reject stale writers after leadership or lease changes | Test clock skew and split-brain scenarios |
| MZG-019 | Per-object history-retention policy | Retain diffs according to object-specific read requirements | Measure state bytes and rejected `AS OF` reads |
| MZG-020 | Upper-frontier API for every maintained object | Expose safe read and compaction bounds uniformly | Verify no read observes data beyond its frontier |
| MZG-021 | Historical `AS OF` reads from persisted diffs | Serve bounded point-in-time queries without full replay | Compare read latency, retained bytes, and correctness |
| MZG-022 | Logical timestamp range planning | Prune persisted updates outside requested temporal bounds | Measure bytes read and plan overhead |
| MZG-023 | Differential join delta reuse | Reuse indexed join state for repeated update keys | Compare update CPU and arrangement memory |
| MZG-024 | Semijoin reduction before remote reads | Push key existence filters toward sources | Measure source bytes and false-positive work |
| MZG-025 | Recursive fixpoint operator wiring | Compile recursive SQL into the bounded fixpoint scheduler | Test termination, duplicate coalescing, and work limits |
| MZG-026 | General negative-diff operator compiler | Lower filter/map/set/join changes across supported SQL shapes | Differentially compare full recomputation |
| MZG-027 | Retractable grouped aggregate kernels | Update aggregate state from signed changes without rebuilds | Measure update CPU, state bytes, and exact results |
| MZG-028 | Incremental top-k maintenance | Maintain ordered winners under inserts and retractions | Compare update CPU and tie-break stability |
| MZG-029 | Incremental window maintenance | Update only affected frames and partitions | Measure state bytes, update CPU, and boundary cases |
| MZG-030 | Snapshot-then-live CDC connector contract | Standardize initial snapshot plus contiguous live tail | Test restart and source-offset recovery |
| MZG-031 | Online dataflow repartitioning | Move keyed state between workers while retaining progress | Measure pause time, transfer bytes, and consistency |
| MZG-032 | Compute/storage separation for persisted arrangements | Fetch cold state from shared storage on demand | Measure network bytes, hydration latency, and memory |
| MZG-033 | Read-replica routing by arrangement locality | Route reads to replicas that already hold required state | Compare p99 latency and rebalance cost |
| MZG-034 | Replica autoscaling from frontier lag | Add/remove compute capacity from lag and memory signals | Measure lag recovery and churn |
| MZG-035 | Per-object state memory budgets | Admit, compress, spill, or reject state by object budget | Verify bounded RSS and useful error reporting |
| MZG-036 | Remote spill tier for arrangements | Spill cold arrangement batches to object storage | Compare memory, read latency, and bandwidth |
| MZG-037 | Automatic dictionary-compression admission | Enable compression only below a measured cardinality threshold | Compare retained bytes, rebuild peak, and lookup CPU |
| MZG-038 | Arrangement metrics keyed by logical operator | Attribute memory, updates, and compaction to dataflow nodes | Verify low-cardinality labels and metric overhead |
| MZG-039 | Dataflow graph `EXPLAIN` with arrangement edges | Show operator reuse, arrangement keys, and frontier waits | Compare explain cost and diagnostic completeness |
| MZG-040 | Cooperative dataflow cancellation | Propagate cancellation through sources, operators, and sinks | Measure shutdown latency and leaked work |
| MZG-041 | Transactional catalog/dataflow deployment | Apply catalog and dataflow changes atomically | Test crash recovery and old-plan availability |
| MZG-042 | Dependency invalidation with version fencing | Recompile only objects affected by schema or source changes | Measure rebuild scope and stale-plan rejection |
| MZG-043 | Source schema evolution compatibility modes | Add/rename/type-change columns with explicit policies | Differentially test old readers and new writers |
| MZG-044 | Dual-write type migration | Populate old and new representations during bounded migration | Measure write amplification and cutover correctness |
| MZG-045 | Temporal `AS OF` joins | Align both sides of a join to a common safe timestamp | Compare frontier wait and result determinism |
| MZG-046 | Subscription resume tokens with retention validation | Return a signed/versioned resume point and reject expired history | Test tampering, expiry, and replay boundaries |
| MZG-047 | Read-hold leases with automatic expiry | Prevent leaked readers from blocking compaction forever | Measure lease overhead and safe expiry behavior |
| MZG-048 | Source freshness SLA and lag alerts | Track frontier age per source and expose bounded alerts | Measure alert accuracy and metric cardinality |
| MZG-049 | Deterministic differential replay harness | Replay signed update traces against full recomputation | Compare outputs and use it as a regression oracle |
| MZG-050 | Multi-region partitioned dataflow routing | Keep regional state local while coordinating global frontiers | Measure cross-region bytes and failover correctness |

## Tarantool: 50 Gaps

| ID | Missing capability | Adoption shape | Measure and rollback gate |
| --- | --- | --- | --- |
| TTG-001 | Per-partition synchronous replication | Require quorum only for selected critical partitions | Compare commit latency and durability behavior |
| TTG-002 | Raft-style leader election and fencing | Elect one writer and reject stale leaders | Test partitions, fencing, and failover latency |
| TTG-003 | Replica bootstrap and resumable rejoin | Stream snapshot plus WAL tail with a checkpoint | Measure catch-up time and transferred bytes |
| TTG-004 | Virtual-bucket sharding | Route keys through stable virtual partitions | Measure distribution, lookup hops, and rebalance cost |
| TTG-005 | Online bucket migration | Transfer a bucket while reads continue and writes fence safely | Measure pause, duplicate work, and final ownership |
| TTG-006 | Router retry and idempotency contract | Retry transient shard failures with request tokens | Test exactly-once mutation outcomes |
| TTG-007 | Per-space storage-engine selection | Choose memory, log-structured, or columnar persistence per space | Compare memory, write throughput, and recovery |
| TTG-008 | Log-structured on-disk space | Add immutable runs, compaction, and indexed reads | Measure write amplification, read latency, and disk bytes |
| TTG-009 | Columnar space for analytical scans | Store typed columns with vectorized projections | Compare scan CPU, memory, and point lookup cost |
| TTG-010 | Multi-part TREE secondary indexes | Index ordered composite keys with partial-prefix lookup | Measure point/range lookup and update maintenance |
| TTG-011 | HASH, RTREE, and BITSET index families | Add specialized indexes for equality, geometry, and flags | Benchmark selectivity, memory, and fallback correctness |
| TTG-012 | Partial/functional secondary indexes | Index only rows matching a predicate or computed key | Measure maintenance cost and invalidation correctness |
| TTG-013 | Cross-partition uniqueness enforcement | Coordinate unique keys without central full scans | Compare write latency and conflict behavior |
| TTG-014 | Parallel secondary-index build | Sort and build independent index ranges concurrently | Measure build wall time, peak memory, and determinism |
| TTG-015 | Transaction savepoints | Roll back part of an atomic batch without aborting all work | Test nested rollback and WAL output |
| TTG-016 | Long-running MVCC transactions | Provide stable read views while writers continue | Measure version retention and conflict rate |
| TTG-017 | Transaction conflict diagnostics | Expose conflicting keys, readers, and retry guidance | Measure diagnostic overhead and privacy boundaries |
| TTG-018 | `on_replace` mutation hooks | Deliver ordered row-change callbacks with bounded backpressure | Compare write latency and dropped-hook behavior |
| TTG-019 | Schema/watch notifications | Notify clients when spaces/indexes/configuration change | Measure notification fanout and reconnect behavior |
| TTG-020 | Cooperative fiber scheduler | Run background maintenance with explicit yield budgets | Measure foreground tail latency and fairness |
| TTG-021 | Fiber cancellation propagation | Cancel blocked work and release all owned resources | Test cancellation latency and leak detection |
| TTG-022 | Bounded fiber channels | Provide allocation-bounded producer/consumer queues | Compare throughput, backpressure, and memory |
| TTG-023 | WAL group commit policy | Batch fsyncs by size or latency budget | Measure durability window and commit throughput |
| TTG-024 | Configurable WAL sync modes | Select strict, periodic, or deferred durability explicitly | Compare recovery loss window and write latency |
| TTG-025 | Incremental snapshots | Snapshot only changed pages or runs after a base snapshot | Measure snapshot time, bytes, and restore correctness |
| TTG-026 | WAL tail shipping | Stream committed records to a remote recovery target | Measure bandwidth, lag, and replay correctness |
| TTG-027 | Consistent hot backup API | Coordinate snapshot and WAL boundaries without stopping writes | Verify point-in-time restore and foreground impact |
| TTG-028 | Recovery checkpoints | Persist replay progress to resume after interruption | Measure restart time and checkpoint overhead |
| TTG-029 | Compact MessagePack wire mode | Use schema-aware compact encoding for common rows | Compare wire bytes, CPU, and compatibility |
| TTG-030 | Pooled remote connections | Reuse authenticated client connections with health checks | Measure connection churn and p99 request latency |
| TTG-031 | Prepared remote calls | Cache parsed/validated command shapes across requests | Compare CPU, allocations, and invalidation behavior |
| TTG-032 | Ordered stream API | Expose a bounded append/read stream with offsets | Measure throughput, retention, and resume correctness |
| TTG-033 | Request tracing context propagation | Carry trace/request IDs through local and remote calls | Measure overhead and sampling correctness |
| TTG-034 | Per-space/index metrics | Attribute reads, writes, misses, and bytes to schema objects | Bound label cardinality and metric cost |
| TTG-035 | Reliable queue with ready/in-flight/ack states | Persist delivery state and reclaim timed-out work | Test crash recovery and duplicate delivery |
| TTG-036 | Priority queue index | Order ready work by priority and stable sequence | Compare dequeue CPU and starvation |
| TTG-037 | Delayed queue index | Make not-before timestamps queryable without polling scans | Measure timer overhead and due-item latency |
| TTG-038 | Queue deduplication token | Collapse duplicate submissions while retaining result state | Test token conflicts and retention |
| TTG-039 | Per-space TTL expiration scheduler | Expire rows without full-space scans | Compare CPU, deletion lag, and memory |
| TTG-040 | Rate-limited trigger delivery | Bound hook/notification work per tenant or space | Measure backlog and foreground isolation |
| TTG-041 | Online schema migration versioning | Track compatible schema generations and readers | Test mixed-version reads/writes and rollback |
| TTG-042 | Online secondary-index change | Build an index in the background and atomically publish it | Measure write overhead and cutover pause |
| TTG-043 | Atomic DDL journal | Persist schema changes as replayable transactions | Verify crash recovery and catalog consistency |
| TTG-044 | Replication-local spaces | Keep node-local metadata out of replicated data paths | Measure replication bytes and failover semantics |
| TTG-045 | Multi-datacenter replication modes | Select async, quorum, or region-local durability per space | Compare WAN latency, loss window, and conflict handling |
| TTG-046 | Conflict-resolution trigger framework | Resolve divergent writes with deterministic user policy | Test replay determinism and bounded execution |
| TTG-047 | Read consistency modes | Choose local, leader, quorum, or bounded-stale reads | Measure latency and stale-read bounds |
| TTG-048 | WAL-driven cache invalidation | Publish key/version invalidations to local caches | Compare hit rate, wire bytes, and stale windows |
| TTG-049 | Stored-function execution limits | Run server-side functions with CPU, memory, and cancellation bounds | Test isolation and per-call overhead |
| TTG-050 | Fiber and memory profiler snapshots | Capture low-overhead hotspots by request and space | Measure profiler overhead and report usefulness |

## Delivery Order

The first implementation candidates should be isolated, measurable features
with clear fallback paths: `CHG-001`, `MZG-001`, `TTG-037`, `CHG-006`, and
`MZG-009`. Distributed replication and storage-engine work should wait until
the local correctness and operational contracts are explicit. Each completed
row must link its focused test, benchmark, and commit from the repository's
existing inspiration ledger.
