# Engine Inspiration Gaps - Round 16

Audit date: 2026-10-01

This is a concrete gap catalog for ideas inspired by ClickHouse, Materialize,
and Tarantool. The audit compared each item with the current public SQL,
columnar, storage, replication, and operations surfaces. An item can have
related primitives already present; `OPEN` means the complete capability below
was not found. The catalog is intentionally specific so future work does not
re-propose features that already exist under a different name.

The implementation rule for this round is test first, benchmark before and
after, keep the change only when correctness is preserved and the measured
tradeoff is acceptable, then commit and push the feature.

## Round 16 Result

| Item | Result | Before | After |
| --- | --- | --- | --- |
| CH-01 columnar `LIMIT WITH TIES` | Implemented | Specialized path returned only `LIMIT` rows: 6.13 ms, 174.5 KB, 20,077 allocs; correct generic fallback: 36.4 ms, 18.6 MB, 100,043 allocs | Correct columnar path: 6.82 ms, 176.7 KB, 20,101 allocs |

The benchmark uses 20,000 rows, a descending numeric order, and `LIMIT 20
WITH TIES`, repeated five times with `-benchtime=250ms`. The new path is about
5.3x faster and 105x lower in measured heap than the correct generic fallback.
It is about 11% slower than the old specialized path, but that path was
incorrect because it silently dropped ties. The extra cost is a second narrow
sort-key scan; projected rows are still materialized only after ranking.

## ClickHouse Candidates

Source references: [query optimization guide][CH-G], [lazy materialization][CH-L],
[projections and secondary indexes][CH-P], [parallel replicas][CH-R], and
[feature journey][CH-F].

| ID | Concrete gap candidate | Current audit | Status |
| --- | --- | --- | --- |
| CH-01 | Correct columnar `LIMIT WITH TIES` without wide-row materialization | Specialized Top-N capped the page and dropped ties | DONE in round 16 |
| CH-02 | Tie-aware sorted-column projection fast path | Sorted projection helpers still stop at the page limit | OPEN |
| CH-03 | Adaptive mark granularity selected from compressed bytes | No runtime granularity controller found | OPEN |
| CH-04 | Per-column granularity skew report | No mark-size skew diagnostic found | OPEN |
| CH-05 | Workload-driven skip-index recommendation | Advisors cover other index choices, not automatic skip-index creation | OPEN |
| CH-06 | Disjunctive `OR` skip-index pruning | Current pruning is narrower than general disjunctions | OPEN |
| CH-07 | Skip-index false-positive telemetry | No per-index false-positive counter found | OPEN |
| CH-08 | Token bloom filter text index | No token-bloom index implementation found | OPEN |
| CH-09 | Stemmed text index | No language-stemming index path found | OPEN |
| CH-10 | JSON all-values text index | JSON subcolumns exist, but no all-values text index found | OPEN |
| CH-11 | Dynamic JSON-path bloom filtering | No path-discovery bloom index found | OPEN |
| CH-12 | Automatic skip-index expression selection | No expression search over observed predicates found | OPEN |
| CH-13 | Online skip-index backfill progress | No per-index backfill progress surface found | OPEN |
| CH-14 | Background projection refresh queue | Projection work is not exposed as a refresh queue | OPEN |
| CH-15 | Build projections lazily after first qualifying query | No on-demand projection build lifecycle found | OPEN |
| CH-16 | Projection freshness and coverage metrics | No combined freshness/coverage metric found | OPEN |
| CH-17 | Projection selection by estimated bytes read | Cost analysis exists, but runtime byte-read selection is incomplete | OPEN |
| CH-18 | Result-cache invalidation by mutation sequence | Cache invalidation is not tied to a storage mutation sequence | OPEN |
| CH-19 | Result-cache admission by measured benefit | No benefit-based admission policy found | OPEN |
| CH-20 | Shared compressed result-cache payloads | Cache entries are not shared as compressed wire blocks | OPEN |
| CH-21 | Durable async-insert deduplication tokens | Async insert exists without a durable token log | OPEN |
| CH-22 | Materialized-view deduplication for async inserts | No insert-token propagation into view maintenance found | OPEN |
| CH-23 | Async-insert age-based flushing | Flush policy is not exposed as an age/size controller | OPEN |
| CH-24 | Async-insert backpressure reason metrics | No reason-coded queue pressure metrics found | OPEN |
| CH-25 | Lightweight update patch chains | No patch-part update representation found | OPEN |
| CH-26 | Delete bitmap compaction thresholds | No explicit bitmap debt/threshold scheduler found | OPEN |
| CH-27 | Mutation cancellation checkpoints | No user-visible mutation checkpoint cancellation found | OPEN |
| CH-28 | Mutation dependency scheduling | No dependency-aware mutation queue found | OPEN |
| CH-29 | TTL action cost scheduling | TTL work is not scheduled by estimated action cost | OPEN |
| CH-30 | Zero-copy part cloning | No metadata-only part clone operation found | OPEN |
| CH-31 | Per-part checksum verification command | No operator command for targeted part verification found | OPEN |
| CH-32 | Automatic merge-pool sizing | No feedback controller for background merge workers found | OPEN |
| CH-33 | Small-part consolidation policy | No explicit small-part debt policy found | OPEN |
| CH-34 | Stable parallel-replica granule partitioning | Replica work assignment is not exposed as a stable partition map | OPEN |
| CH-35 | Parallel-replica lag fallback | No automatic fallback threshold based on replica lag found | OPEN |
| CH-36 | Parallel-replica progress barrier | No query-level completion barrier metric found | OPEN |
| CH-37 | Distributed retry budget | No per-query retry budget across remote fragments found | OPEN |
| CH-38 | Remote exchange compression negotiation | No negotiated block codec for query exchange found | OPEN |
| CH-39 | Pipeline queue occupancy metrics | Operator queues lack a standard occupancy metric | OPEN |
| CH-40 | Operator-level allocation counters | No allocation counter per pipeline operator found | OPEN |
| CH-41 | Vectorized substring search | No SIMD substring kernel for columnar strings found | OPEN |
| CH-42 | Block-scoped expression arena reuse | No reusable expression arena per block found | OPEN |
| CH-43 | Aggregate-state spill format | No dedicated compact aggregate-state spill codec found | OPEN |
| CH-44 | Automatic two-level aggregation threshold | No adaptive bucket threshold based on cardinality found | OPEN |
| CH-45 | Sparse group-key storage selection | No density-based sparse/dense key switch found | OPEN |
| CH-46 | Shared low-cardinality dictionaries across parts | Dictionaries remain part-scoped in the audited surface | OPEN |
| CH-47 | Dictionary remap compaction | No background dictionary remap debt policy found | OPEN |
| CH-48 | Per-column codec autotuning from entropy | No observed-entropy codec controller found | OPEN |
| CH-49 | Range-checksum read verification | No checksum verification limited to selected mark ranges found | OPEN |
| CH-50 | Normalized workload log of top byte offenders | No normalized read-byte offender report found | OPEN |

## Materialize Candidates

Source references: [arrangements][MZ-A], [optimization][MZ-O], [indexes][MZ-I],
[partition and filter pushdown][MZ-P], and [dictionary compression][MZ-D].

| ID | Concrete gap candidate | Current audit | Status |
| --- | --- | --- | --- |
| MZ-01 | Per-arrangement history-retention lease | No lease separate from global retention was found | OPEN |
| MZ-02 | Declarative per-index `RETAIN HISTORY` policy | No index-specific history policy was found | OPEN |
| MZ-03 | Stale-read freshness bound | No query API for bounded arrangement staleness found | OPEN |
| MZ-04 | Frontier-lag query admission | No admission controller based on dataflow lag found | OPEN |
| MZ-05 | Arrangement hydration progress API | No percentage/ETA hydration surface found | OPEN |
| MZ-06 | Rehydrate an arrangement without upstream replay | No persisted-arrangement-only restore path found | OPEN |
| MZ-07 | Index placement per compute cluster | Index placement is not independently assignable | OPEN |
| MZ-08 | Arrangement resource-pool isolation | No per-arrangement memory/CPU pool found | OPEN |
| MZ-09 | Arrangement memory attribution | No per-arrangement retained-byte metric found | OPEN |
| MZ-10 | Index size and cardinality advisor | No index advisor for arrangement size found | OPEN |
| MZ-11 | `EXPLAIN` arrangement reuse | No plan detail showing reused arrangement identity found | OPEN |
| MZ-12 | `EXPLAIN` index key coverage | No output showing which requested keys are covered found | OPEN |
| MZ-13 | Online index-build percentage | No index build progress stream found | OPEN |
| MZ-14 | Safe index-drop dependency report | No dependency report before dropping an index found | OPEN |
| MZ-15 | `DISTINCT ON INPUT GROUP SIZE` optimizer hint | No equivalent hint found | OPEN |
| MZ-16 | `AGGREGATE INPUT GROUP SIZE` optimizer hint | No equivalent hint found | OPEN |
| MZ-17 | `LIMIT INPUT GROUP SIZE` optimizer hint | No equivalent hint found | OPEN |
| MZ-18 | Inequality-join arrangement reuse | No range-join arrangement reuse path found | OPEN |
| MZ-19 | Prefix-key arrangement sharing | No automatic prefix arrangement sharing found | OPEN |
| MZ-20 | Cross-view arrangement sharing | No shared arrangement dependency graph found | OPEN |
| MZ-21 | Partition-key co-location for arrangements | No co-location policy found | OPEN |
| MZ-22 | Partition-pruning cost model | No partition pruning cost estimate found | OPEN |
| MZ-23 | Partition-aware source snapshot barrier | No per-partition snapshot barrier found | OPEN |
| MZ-24 | Antichain-density compaction trigger | No density-based frontier compaction trigger found | OPEN |
| MZ-25 | Diff coalescing before arrangement insertion | No pre-arrangement update coalescer found | OPEN |
| MZ-26 | Duplicate retraction suppression | No duplicate negative-diff suppression path found | OPEN |
| MZ-27 | Upsert-key migration accounting | No metric for keys changing their current value found | OPEN |
| MZ-28 | Temporal index retention policy | No retention policy tied to logical time was found | OPEN |
| MZ-29 | Durable subscription resume token | No durable token for resuming a subscription found | OPEN |
| MZ-30 | Subscription pause/resume at frontier | No operator pause contract tied to frontier found | OPEN |
| MZ-31 | Subscription progress record format | No stable progress-record schema found | OPEN |
| MZ-32 | Sink checkpoint introspection | No per-sink checkpoint diagnostic found | OPEN |
| MZ-33 | Sink exactly-once replay guard | No sink-side replay guard keyed by checkpoint found | OPEN |
| MZ-34 | Source schema-evolution barrier | No schema change barrier across dataflow workers found | OPEN |
| MZ-35 | Connector snapshot throttle control | No connector-specific snapshot throttle found | OPEN |
| MZ-36 | Source backpressure attribution | No source-to-operator backpressure attribution found | OPEN |
| MZ-37 | Operator critical-path report | No dataflow critical-path report found | OPEN |
| MZ-38 | Worker scheduling fairness | No fairness policy for competing dataflow operators found | OPEN |
| MZ-39 | Compute-replica isolation | No per-replica workload isolation control found | OPEN |
| MZ-40 | Failed-worker arrangement rehydration metric | No recovery metric separating replay from hydrate found | OPEN |
| MZ-41 | Persistent arrangement compaction | No durable arrangement compaction worker found | OPEN |
| MZ-42 | Per-arrangement dictionary auto mode | No per-arrangement compression mode chooser found | OPEN |
| MZ-43 | Dictionary miss/fallback metrics | No dictionary fallback counter found | OPEN |
| MZ-44 | Partial index hydration | No selective key-range hydration found | OPEN |
| MZ-45 | Live compute-cluster migration | No arrangement migration workflow found | OPEN |
| MZ-46 | Cancellation propagation latency | No end-to-end cancellation latency metric found | OPEN |
| MZ-47 | Optimizer feedback from observed rows | No feedback loop from actual row counts found | OPEN |
| MZ-48 | Freshness and reaction-time SLO metrics | No combined freshness/reaction-time SLO found | OPEN |
| MZ-49 | Workload-driven index retirement | No automatic unused-index retirement recommendation found | OPEN |
| MZ-50 | Arrangement-key join advisor | No advisor for choosing reusable join keys found | OPEN |

## Tarantool Candidates

Source references: [Tarantool features and SQL][TT-R], [Vinyl architecture][TT-V],
[memory calculation][TT-M], [pagination position API discussion][TT-P],
[SQL `ON CONFLICT` design][TT-C], and [official source tree][TT-S].

| ID | Concrete gap candidate | Current audit | Status |
| --- | --- | --- | --- |
| TT-01 | Opaque ordered-index position tokens | No compatible position-token API found | OPEN |
| TT-02 | Mutation-safe position pagination | No resume semantics that survive index mutations found | OPEN |
| TT-03 | Covering-index projection reads | No index-only projection path found | OPEN |
| TT-04 | Functional indexes | No expression-backed index definition found | OPEN |
| TT-05 | Multikey JSON-path indexes | No multikey path index found | OPEN |
| TT-06 | Tuple-field collation indexes | No persisted per-field collation index found | OPEN |
| TT-07 | Hash collision and chain statistics | No hash-index collision telemetry found | OPEN |
| TT-08 | Bitset indexes | No bitset index implementation found | OPEN |
| TT-09 | R-tree nearest-neighbor queries | No spatial nearest-neighbor index found | OPEN |
| TT-10 | Contract-tested index iterator types | No compatibility test matrix for all iterator modes found | OPEN |
| TT-11 | Range count without scanning tuples | No metadata range-count path found | OPEN |
| TT-12 | Online index-build progress | No stable build percentage API found | OPEN |
| TT-13 | Online index-build throttling | No load-aware build throttle found | OPEN |
| TT-14 | Online index-consistency checker | No operator checker for tuple/index divergence found | OPEN |
| TT-15 | Transaction savepoints | No public savepoint API found | OPEN |
| TT-16 | Read-only transaction snapshots | No explicit read-only snapshot mode found | OPEN |
| TT-17 | Historical MVCC reads | No timestamped historical tuple read found | OPEN |
| TT-18 | WAL group-commit metrics | No group-commit latency/size metric found | OPEN |
| TT-19 | WAL retention by replication LSN | No LSN retention policy surface found | OPEN |
| TT-20 | Incremental LSN backup | No backup API for LSN ranges found | OPEN |
| TT-21 | Backup manifest checksums | No manifest with per-file checksums found | OPEN |
| TT-22 | Hot read-only standby | No promoted/read-only standby workflow found | OPEN |
| TT-23 | Synchronous quorum replication | No write quorum durability mode found | OPEN |
| TT-24 | Automatic Raft failover | No automatic leader failover workflow found | OPEN |
| TT-25 | Vclock conflict-resolution policy | No configurable conflict policy found | OPEN |
| TT-26 | Replication idempotency guard by LSN | No applied-LSN deduplication metric found | OPEN |
| TT-27 | Leader lease | No bounded leader lease API found | OPEN |
| TT-28 | Fiber cancellation hooks | No public cancellation hook contract found | OPEN |
| TT-29 | Roll back a transaction on fiber cancellation | No cancellation-to-rollback guarantee found | OPEN |
| TT-30 | Channel/select synchronization primitive | No general select-style fiber primitive found | OPEN |
| TT-31 | Net.box batch request pipeline | No explicit pipeline batching API found | OPEN |
| TT-32 | Wire compression negotiation | No negotiated client/server wire codec found | OPEN |
| TT-33 | Prepared SQL statement cache | No reusable prepared-statement cache found | OPEN |
| TT-34 | Schema-version plan invalidation | No plan invalidation event keyed by schema version found | OPEN |
| TT-35 | `ON CONFLICT` diagnostic detail | No per-row conflict/index diagnostic found | OPEN |
| TT-36 | Tuple field update operations | No allocation-free tuple patch operation found | OPEN |
| TT-37 | Before/after trigger phases | No separate trigger phase contract found | OPEN |
| TT-38 | Online space-format migration | No resumable format migration workflow found | OPEN |
| TT-39 | Schema-version handshake | No client schema compatibility handshake found | OPEN |
| TT-40 | Tuple arena offset accounting | No per-space tuple-arena accounting found | OPEN |
| TT-41 | Slab allocator class metrics | No slab-class fragmentation report found | OPEN |
| TT-42 | Vinyl bloom-filter auto-sizing | No observed-FPR bloom sizing controller found | OPEN |
| TT-43 | Vinyl tuple-cache persistence | No restart-persistent hot tuple cache found | OPEN |
| TT-44 | Vinyl tombstone-GC watermark | No operator-visible tombstone watermark found | OPEN |
| TT-45 | Vinyl read-amplification metrics | No per-range read-amplification metric found | OPEN |
| TT-46 | Snapshot throttling | No snapshot worker bandwidth controller found | OPEN |
| TT-47 | WAL fsync-mode observability | No runtime metric explaining fsync mode cost found | OPEN |
| TT-48 | Remote cold-storage tier | No object-storage tier for cold Vinyl data found | OPEN |
| TT-49 | Space-level TTL scheduler | No per-space TTL scheduler with debt metrics found | OPEN |
| TT-50 | Session memory and work quotas | No per-session memory/work quota surface found | OPEN |

## Follow-up Rule

The next round should select only an `OPEN` item whose current audit is still
true. Every selected item gets a focused failing test, a pre-change benchmark,
the smallest implementation, a correctness/race/vet check, and a post-change
benchmark. A result that loses on the relevant workload without a correctness
or operational benefit should be reverted rather than retained.

[CH-G]: https://clickhouse.com/resources/engineering/clickhouse-query-optimisation-definitive-guide
[CH-L]: https://clickhouse.com/blog/clickhouse-gets-lazier-and-faster-introducing-lazy-materialization
[CH-P]: https://clickhouse.com/blog/projections-secondary-indices
[CH-R]: https://clickhouse.com/blog/clickhouse-parallel-replicas
[CH-F]: https://clickhouse.com/clickhouse/feature-journey
[MZ-A]: https://materialize.com/docs/fundamentals/concepts/arrangements/
[MZ-O]: https://materialize.com/docs/transform-data/optimization/
[MZ-I]: https://materialize.com/docs/fundamentals/concepts/indexes/
[MZ-P]: https://materialize.com/docs/transform-data/patterns/partition-by/
[MZ-D]: https://materialize.com/docs/transform-data/dictionary-compression/
[TT-R]: https://github.com/tarantool/tarantool
[TT-V]: https://github.com/tarantool/tarantool/wiki/Vinyl-Architecture
[TT-M]: https://github.com/tarantool/tarantool/wiki/Memory-size-calculation
[TT-P]: https://github.com/tarantool/tarantool/issues/7639
[TT-C]: https://github.com/tarantool/tarantool/wiki/SQL%3A-ON-CONFLICT-clause-for-INSERT%2C-UPDATE-statements
[TT-S]: https://github.com/tarantool/tarantool/tree/master/src/box
