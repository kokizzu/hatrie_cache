# ClickHouse, Materialize, And Tarantool Gap Catalog

This is the next implementation queue for ideas found in ClickHouse,
Materialize, and Tarantool that are not yet complete public capabilities in
this repository. The existing [CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md](CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md)
tracks the original 50 canonical ideas per product; this catalog deliberately
uses new `G*` identifiers so an implementation is not mistaken for an already
completed canonical item.

Each item is a candidate, not a promise to copy another product's semantics.
Before implementation, the item must pass a local API/code search, receive a
focused red test, and have a baseline benchmark. A feature stays opt-in unless
the measured result is positive and the compatibility surface is clear. A
feature is rolled back when correctness regresses, the common path slows, or
the memory/operational cost is disproportionate to the gain.

## ClickHouse: 50 Candidate Gaps

| ID | Candidate idea not yet complete here | First verification gate |
| --- | --- | --- |
| CH-G01 | Adaptive join-algorithm selection among in-memory hash, grace-hash, and partial-merge paths | Same result rows; compare CPU, spill bytes, and peak memory |
| CH-G02 | Grace-hash join partition spill with bounded disk space | Fault-injected spill/reload and bounded peak memory |
| CH-G03 | Parallel hash-join build/probe partitions with deterministic output merge | Compare against serial join on duplicate and NULL keys |
| CH-G04 | Adaptive runtime Bloom precheck for large numeric/boolean hash joins; lazy admission after a miss-heavy probe sample | Implemented and measured in `BENCHMARK.md`: 1.53x miss-heavy microbenchmark win, 3.021% false-positive rate, no small-index allocation |
| CH-G05 | Runtime min/max filters propagated from a small join side to remote partitions | Verify no false negatives across partition boundaries |
| CH-G06 | Join-side prefetch scheduling based on mark selectivity | Compare read amplification and tail latency |
| CH-G07 | SQL `GROUPING SETS`, `ROLLUP`, and `CUBE` over typed grouped execution | Differential result tests including empty groups |
| CH-G08 | Explicit aggregate totals rows with a stable totals policy | Verify totals with LIMIT, HAVING, and empty input |
| CH-G09 | Per-group `LIMIT BY` execution using bounded top-k state | Compare memory with full sort and tie ordering |
| CH-G10 | General `ARRAY JOIN` expansion with typed nested columns | Validate NULL, empty-array, and row-multiplication semantics |
| CH-G11 | Multi-key ASOF join merge path with bounded temporal search | Compare exact matches and out-of-order inputs |
| CH-G12 | Exact `INTERSECT ALL` and `EXCEPT ALL` signed multiplicity operators | Differential multiset and duplicate tests |
| CH-G13 | SQL row-pattern matching equivalent to `MATCH_RECOGNIZE` for bounded streams | Verify deterministic state transitions and memory cap |
| CH-G14 | Heavy-hitter `topK` aggregate state with mergeable bounded counters | Measure error bounds, merge cost, and retained bytes |
| CH-G15 | Streaming histogram aggregate with mergeable fixed bucket state | Compare bucket boundaries and serialized state size |
| CH-G16 | Adaptive distinct aggregate choosing exact, bitmap, or sketch state | Verify exactness mode transitions and error reporting |
| CH-G17 | Reservoir-sampled `groupArray` aggregate with deterministic seed | Compare sample stability after partial-state merge |
| CH-G18 | Aggregate combinator registry for conditional, array, map, and state variants | API compatibility and zero-allocation hot path for common kinds |
| CH-G19 | Nested array-of-struct columnar layout with child-column pruning | Compare wide-row read bytes against flat fallback |
| CH-G20 | Dynamic JSON subcolumn promotion from observed hot paths | Verify schema generation and fallback for unseen paths |
| CH-G21 | Cross-part low-cardinality dictionary compaction with code remapping | Ensure old encoded parts remain readable during rebuild |
| CH-G22 | Online Enum dictionary extension with versioned wire compatibility | Reject incompatible values without mutating live state |
| CH-G23 | Specialized UUID fixed-width ordering and RowBinary codec | Benchmark lookup/sort against string representation |
| CH-G24 | Specialized DateTime64 timezone conversion kernel | Compare correctness around DST and allocation count |
| CH-G25 | IPv6 prefix index with longest-prefix lookup | Verify canonical address parsing and prefix precedence |
| CH-G26 | Variant-path statistics used for selective JSON subcolumn reads | Compare scan bytes and false-pruning safety |
| CH-G27 | Partition-key advisor using observed cardinality and time locality | Advisor-only output; no automatic layout mutation |
| CH-G28 | Partition-size split/merge recommendations with hysteresis | Prevent oscillation and measure recommendation stability |
| CH-G29 | Mutation dependency cancellation and explicit kill state transitions | Verify durable state after restart and cancellation races |
| CH-G30 | Delete-bitmap compaction triggered by density and read amplification | Compare lookup cost and retained bitmap bytes |
| CH-G31 | Consistent hard-link style local snapshot freeze for immutable parts | Fault injection and manifest consistency validation |
| CH-G32 | Backup upload scheduler with per-destination concurrency and throttling | Compare throughput, peak memory, and fairness |
| CH-G33 | Remote-part cache stale-while-revalidate admission | Verify checksum safety and stale-hit policy |
| CH-G34 | Mark-aware remote-part prefetch priority queue | Measure latency and bytes fetched for selective scans |
| CH-G35 | Remote immutable-part replication with resumable range copy | Verify partial retry and final checksum equality |
| CH-G36 | Query-result cache keyed by normalized AST and dependency tags | Ensure literal normalization never aliases unsafe queries |
| CH-G37 | Query fingerprint and bounded normalized query log | Measure log bytes and privacy redaction |
| CH-G38 | Per-thread/operator query log with phase timing | Verify bounded cardinality and aggregation cost |
| CH-G39 | Asynchronous metric history with fixed retention and downsampling | Measure metric overhead and restart behavior |
| CH-G40 | Sampling memory profiler with bounded stack aggregation | Compare profiler overhead against disabled control |
| CH-G41 | Per-query thread budget with cooperative operator admission | Verify fairness and cancellation latency |
| CH-G42 | Parallel-replica read coordination with deterministic shard ranges | Compare latency, duplicate reads, and failure fallback |
| CH-G43 | Distributed `IN` set shipping with compressed typed payloads | Measure wire bytes and build CPU |
| CH-G44 | Distributed join broadcast threshold based on observed serialized size | Verify plan stability at threshold boundaries |
| CH-G45 | Versioned external dictionary reload with atomic reader handoff | Ensure readers see one complete version only |
| CH-G46 | Dictionary negative-cache entries with bounded expiry | Implemented in [CH046_DICTIONARY_NEGATIVE_CACHE.md](CH046_DICTIONARY_NEGATIVE_CACHE.md); default off and benchmarked in [BENCHMARK.md](BENCHMARK.md#ch046-dictionary-negative-cache) |
| CH-G47 | Kafka-style transactional source offset plus data checkpoint | Fault-injected restart must not duplicate committed data |
| CH-G48 | Live result view refresh notifications with dependency frontier | Verify no stale notification after invalidation |
| CH-G49 | Asynchronous read-ahead budget shared across columns and parts | Compare tail latency and cache pollution |
| CH-G50 | Query plan reproducibility hash with settings and schema fingerprint | Implemented and measured in `CH050_PLAN_REPRODUCIBILITY_HASH.md` and `BENCHMARK.md`: bounded structural SHA-256 hash with runtime-field exclusion |

## Materialize: 50 Candidate Gaps

| ID | Candidate idea not yet complete here | First verification gate |
| --- | --- | --- |
| MZ-G01 | Reusable SQL-to-dataflow fragment lowering with parameterized inputs | Same plan semantics and lower compile time on repeated queries |
| MZ-G02 | Correlated-subquery decorrelation into incremental joins | Compare results with NULL and duplicate outer rows |
| MZ-G03 | Incremental lateral/table-function join maintenance | Bounded state and exact insert/retract behavior |
| MZ-G04 | Generic recursive dataflow maintenance with retractions | Termination bound and cycle-safe differential results |
| MZ-G05 | Generic threshold/negation operator for signed differential collections | Verify frontier and negative-diff semantics |
| MZ-G06 | Partial-order antichain frontiers for arbitrary timestamp dimensions | Compare readiness and compaction decisions |
| MZ-G07 | Automatic logical-compaction tuning from active read holds | No compaction past any live hold |
| MZ-G08 | Arrangement warmup priority from observed query heat | Measure hydration latency without hurting steady-state reads |
| MZ-G09 | Equivalent arrangement-key sharing across different index declarations | Verify key canonicalization and result identity |
| MZ-G10 | Adaptive arrangement key reordering from workload statistics | Advisor-only first; compare lookup/read amplification |
| MZ-G11 | Delta-join path switching as input skew changes | No duplicate/retracted output during transition |
| MZ-G12 | Hot-key replication in skew-aware join exchange | Compare skew tail latency and retained bytes |
| MZ-G13 | Exchange backpressure with per-key fairness | Bounded queues and no starvation under one hot key |
| MZ-G14 | Worker scaling trigger based on frontier lag and utilization | Hysteresis prevents scale thrashing |
| MZ-G15 | Automatic operator yield-budget tuning | Compare throughput and worst-case scheduling delay |
| MZ-G16 | Cancellation propagation across shared dataflows with reference counts | One canceled query must not cancel a shared view |
| MZ-G17 | Durable timeline fork/merge conflict records | Deterministic conflict order and restart recovery |
| MZ-G18 | Portable snapshot tokens with schema and source capability checks | Reject tokens from incompatible timelines |
| MZ-G19 | Parallel source snapshot chunks with per-chunk checksums | Restart one failed chunk without replaying all chunks |
| MZ-G20 | Snapshot deduplication across source partitions | Compare stored bytes and preserve source offsets |
| MZ-G21 | Connector schema drift quarantine with operator approval queue | Invalid rows isolated without blocking healthy partitions |
| MZ-G22 | Connector schema compatibility diff with generated migration plan | No silent widening or narrowing of values |
| MZ-G23 | Connector credential version pinning per source generation | Rotation does not interrupt in-flight reads |
| MZ-G24 | Connector retry scheduler with durable exponential-backoff state | Restart preserves next-attempt deadline |
| MZ-G25 | Sink two-phase retry after ambiguous external commit | Idempotency token and exactly-once audit evidence |
| MZ-G26 | Sink progress freshness frontier separate from delivery count | Distinguish staged, committed, and source-frontier lag |
| MZ-G27 | Compacted subscription batches with explicit progress capabilities | Preserve order and resume token correctness |
| MZ-G28 | Snapshot-free subscription catch-up with bounded replay windows | Memory stays bounded under slow consumers |
| MZ-G29 | AS OF capability validation and explain diagnostics | Reject impossible timestamps before execution |
| MZ-G30 | Per-object memory budgets inherited by all descendant operators | Enforce caps without accounting leaks |
| MZ-G31 | Operator metric cardinality budget and deterministic eviction | Metrics cannot grow with unbounded keys |
| MZ-G32 | Dataflow plan snapshot diff with operator identity matching | Equivalent plans produce stable structural diff |
| MZ-G33 | Observed-cost calibration for arrangement reuse scoring | Advice improves without changing execution by default |
| MZ-G34 | Index advisor build/drop hysteresis and minimum residency | Avoid index churn under noisy workloads |
| MZ-G35 | Hydration burst-cluster admission and automatic retirement | Faster hydration without steady-state resource leak |
| MZ-G36 | Durable arrangement-compaction checkpoints with resume | Crash resumes at a verified input frontier |
| MZ-G37 | Read-hold leak detection with owner and age diagnostics | No false cancellation of active readers |
| MZ-G38 | Source lag threshold policies with alert state transitions | Implemented and measured in `MZ038_SOURCE_LAG_ALERTS.md` and `BENCHMARK.md`: bounded hysteresis registry with atomic snapshot/restore |
| MZ-G39 | Timestamp-oracle lease rotation without halting readers | No duplicate timestamps across lease handoff |
| MZ-G40 | Persisted timeline WAL retention tied to live snapshot tokens | Retention never deletes a required timestamp |
| MZ-G41 | Online dual-write type migration for materialized views | Old and new schemas compare before cutover |
| MZ-G42 | Dataflow replay digest for deterministic recovery verification | Same input frontier yields same digest |
| MZ-G43 | What-if optimizer execution using isolated statistics only | No production state mutation from simulation |
| MZ-G44 | Incremental window-frame maintenance for arbitrary peer-aware RANGE bounds | Exact retractions when peer boundaries move |
| MZ-G45 | Incremental percentile state with bounded mergeable t-digest variants | Quantile error and memory are explicit |
| MZ-G46 | Incremental recursive reachability with bounded negative updates | Prevent unbounded retraction cascades |
| MZ-G47 | Transaction timestamp capability propagation through sinks | Sink cannot acknowledge before required frontier |
| MZ-G48 | Catalog migration barrier with rollbackable generated plans | Mixed-version readers remain valid |
| MZ-G49 | Multi-source snapshot barrier with per-source timeout policy | Partial failure reports exact missing capabilities |
| MZ-G50 | Compute-cluster placement advisor using failure domain and locality cost | Recommendation is deterministic and non-mutating |

## Tarantool: 50 Candidate Gaps

| ID | Candidate idea not yet complete here | First verification gate |
| --- | --- | --- |
| TT-G01 | Full Raft leader-election state machine for durable replica ownership | Fault-injected election and fencing safety |
| TT-G02 | Per-space synchronous replication quorum with commit/rollback | No acknowledged write lost below quorum |
| TT-G03 | Supervised failover policy with strict split-brain fencing | Concurrent leaders are rejected |
| TT-G04 | Hot-standby read promotion with journal-position validation | Promotion refuses stale or divergent state |
| TT-G05 | Resumable replica bootstrap from snapshot plus WAL range | Retry does not duplicate or skip an LSN |
| TT-G06 | Multi-master conflict records with deterministic resolution hooks | Conflict is observable and never silently dropped |
| TT-G07 | WAL request delta encoding for updates and deletes | Compare bytes against full-row records |
| TT-G08 | Bounded key-affine parallel WAL decode and apply | Initial shared-trie implementation rejected: 2.58x slower and 6.26x higher measured bytes; revisit only with shard-local state and a merge design |
| TT-G09 | WAL group-commit policy with latency/throughput controls | Measure fsync count, latency, and loss window |
| TT-G10 | WAL segment index for bounded recovery seek | Implemented and measured in `TTG10_WAL_SEGMENT_SEEK.md` and `BENCHMARK.md`: 10.18x faster near-tail replay, 10.74x lower heap, and 10.45x fewer allocations |
| TT-G11 | Independent WAL segment compression with checksummed frames | Implemented and measured in `TTG11_WAL_SEGMENT_COMPRESSION.md` and `BENCHMARK.md`: CRC-protected Zstandard is the segmented-journal default, with explicit `none` fallback |
| TT-G12 | Parallel snapshot serialization by space or shard | Snapshot digest matches serial output |
| TT-G13 | Incremental snapshot manifests with changed-space tracking | Restore from base plus delta equals full snapshot |
| TT-G14 | Per-space snapshot checksums and verified restore report | All supported types round-trip correctly |
| TT-G15 | Hot backup at a consistent LSN while writes continue | Backup never mixes post-LSN rows into the snapshot |
| TT-G16 | Backup stream throttling with reader/writer fairness | Throughput cap and latency impact are measurable |
| TT-G17 | Backup encryption key rotation across resumable chunks | Old and new keys verify at the intended boundary |
| TT-G18 | Vinyl-style compaction scheduler with debt and priority | Bounded debt and no foreground starvation |
| TT-G19 | Range-read prefetch for ordered on-disk indexes | Compare read amplification and tail latency |
| TT-G20 | Columnar analytical storage path for wide immutable tuples | Compare scan bytes and retained memory |
| TT-G21 | Transaction savepoints with partial rollback | Earlier writes survive later savepoint rollback only |
| TT-G22 | MVCC read views for long-running transactions | Snapshot isolation and conflict behavior are explicit |
| TT-G23 | Transaction conflict diagnostics with key and owner metadata | No sensitive values leak in diagnostics |
| TT-G24 | Transaction deadline and deadlock detection policy | Bounded wait with deterministic abort reason |
| TT-G25 | Deterministic stored-procedure replay envelope | Replay uses data operations, not nondeterministic code |
| TT-G26 | Before/after replacement trigger registry with failure policy | Trigger failure preserves atomic mutation semantics |
| TT-G27 | Durable sequence and auto-increment allocator | Restart and concurrent allocation are monotone |
| TT-G28 | TTL index scheduler with bounded deletion batches | Expiry work cannot starve foreground operations |
| TT-G29 | Partial secondary indexes with predicate recheck | Nonmatching rows never appear in results |
| TT-G30 | Multi-index intersection and union planner | Compare postings versus full scan on selectivity extremes |
| TT-G31 | Parallel sorted secondary-index build with progress checkpoints | Cancel/resume never publishes a partial index |
| TT-G32 | Online index drop dependency validation | Queries fail early or switch to a valid fallback |
| TT-G33 | Space-format version migration with dual readers | Mixed-version rows remain readable during rollout |
| TT-G34 | Compact tuple field-name dictionary shared by spaces | Measure bytes and lookup cost versus strings |
| TT-G35 | Array/multikey index maintenance with duplicate element policy | Exact candidate recheck for repeated values |
| TT-G36 | Fiber fairness budget and cooperative yield diagnostics | One fiber cannot monopolize the transaction loop |
| TT-G37 | Cancellation propagation from client request to fiber work | Canceled work stops before commit |
| TT-G38 | Role/privilege catalog with cached authorization decisions | Revocation invalidates all relevant cache entries |
| TT-G39 | Prepared statement cache keyed by schema generation | Schema change cannot reuse an invalid plan |
| TT-G40 | Multiplexed pipelined request frames with ordered responses | Wire order and cancellation are preserved |
| TT-G41 | Negotiated network compression with per-frame limits | Decompression bombs and oversized frames are rejected |
| TT-G42 | Replication lag adaptive flow control per peer | Implemented and measured in `TTG42_PEER_FLOW_CONTROL.md` and `BENCHMARK.md`: bounded independent hysteresis with zero-observation allocations |
| TT-G43 | Replica-read consistency token for minimum LSN reads | Stale replicas are rejected or retried |
| TT-G44 | System catalog for spaces, indexes, WAL, and replica state | Stable bounded rows and redaction policy |
| TT-G45 | Audit log for schema and data-operation metadata | Audit entries are durable and privacy-safe |
| TT-G46 | Journal-triggered changefeed with resume token and checksum | Restart resumes exactly after the acknowledged token |
| TT-G47 | Queue consumer groups with claim, lease, ack, and dead letter | One message is not concurrently acknowledged twice |
| TT-G48 | Fiber-backed delay scheduler with durable deadlines | Restart preserves due ordering and cancellation |
| TT-G49 | Memory quota by space and transaction with admission metrics | Rejects are deterministic and allocation-bounded |
| TT-G50 | Partition-aware rebalancing with backup and restore checkpoints | Movement is resumable and never loses an owned range |

## Sources And Selection Policy

The product concepts are grounded in the official documentation for
[ClickHouse query optimization](https://clickhouse.com/resources/engineering/clickhouse-query-optimisation-definitive-guide),
[Materialize concepts and arrangements](https://materialize.com/docs/fundamentals/concepts/),
and [Tarantool's platform, transactions, WAL, replication, and indexes](https://www.tarantool.io/en/doc/latest/singlepage/).

The first implementation attempt selected from this catalog was `TT-G08`,
bounded key-affine parallel WAL/journal replay. The shared-trie design was
rejected after measurement; see
[TTG08_PARALLEL_REPLAY_REJECTED.md](TTG08_PARALLEL_REPLAY_REJECTED.md).
