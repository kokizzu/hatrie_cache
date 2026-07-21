# Improvement Report

Date: 2026-07-19

## Scope

This report covers the reliability, observability, benchmark, and replication
auth/transport improvements shipped in this work sequence. It also records the
follow-up batching, wire-format, and multi-node correctness hardening shipped
after the first report pass, plus the final cross-cutting architecture pass for
bounded telemetry, concurrent reads, durability, snapshots, anti-entropy, and
persistent command transport.

## Shipped Improvements

| Commit | Improvement | Result |
| --- | --- | --- |
| `aa54135` | Durable replication outbox | Async replication jobs and dead letters can survive process restart when `REPLICATION_OUTBOX_PATH` is configured. |
| `6c5dc30` | Dirty-key LevelDB sync | Periodic LevelDB persistence can write only changed keys after the initial full save, reducing repeated write work. |
| `9aff288` | Ops-state Prometheus metrics | Storage operations, replication queue state, retries, failures, and circuit-breaker state are visible in `/metrics`. |
| `f3dca88` | Multi-node replication integration test | Two real monitoring servers now verify end-to-end leader write replication. |
| `a2f93d9` | Benchmark artifact generation | Benchmark smoke writes JSON/Markdown/TSV/raw files locally; the later local-verification pass removed hosted workflow publishing. |
| `e36d412` | Artifact-driven benchmark docs | `make benchmark-md` regenerates generated `BENCHMARK.md` regions from benchmark artifacts. |
| `96ed123` | Narrow replication auth token | `REPLICATION_AUTH_TOKEN` authenticates outbound HTTP replication and only permits inbound internal replication commands. |
| `a2ca705` | LevelDB replication outbox backend | Async replication can use a low-overhead LevelDB outbox by default, with JSON retained as a configurable fallback. |
| `bb8b86d` | Batch replication by target | Sync and async replay coalesce multiple payloads for the same target into `INTERNALBATCH`, reducing request count. |
| `e899eb8` | multi-node replication failure tests | Real monitoring-server integration tests now cover unauthorized replication targets and LevelDB outbox replay after restart. |
| `a0c7561` | Document internal replication batching | README, benchmark docs, and report guards now document `INTERNALBATCH` and the latest replication surface. |
| `f34ea71` | native replication batch wire format | Protobuf can carry `CommandRequest.batch` directly, avoiding JSON-string batch entries while keeping legacy `values` decode compatibility. |
| `2c24768` | Preflight replication batches before apply | Batch receivers validate every item before mutation, preventing partial writes when a later batch entry is invalid. |
| `675bccc` | Benchmark replication batching | A 10k-key local SyncAll benchmark now compares batched and unbatched replication request, wire, CPU, heap, and allocation costs. |
| `2f3deb6` | multi-node replication chaos tests | Real-server tests now cover follower promotion after primary offline marking and stale topology fingerprint rejection. |
| `7a69306` | Snapshot replication routing per page | Anti-entropy sync clones topology and election state once per page instead of once per key. |
| `f187b83` | Shared replication batch metadata | `INTERNALBATCHV2` carries source, sequence, and topology metadata once on the envelope with automatic legacy fallback. |
| `d471652` | Direct sync target groups | Sync builds per-target batches directly and avoids an intermediate task list and regrouping pass. |
| `70775ee` | Typed binary replication snapshots | Protobuf replication uses compact binary values, persists them through async outboxes, and retries legacy JSON for older peers. |
| `ae64ce3` | Collection allocation reductions | Two-value set reads, priority-queue string pushes, and plain-string radix prefix responses allocate less while preserving generic fallbacks. |
| `bdf8c70` | Deferred sync batch metadata | Sync keeps child payloads metadata-free and annotates one envelope, materializing per-item metadata only for compatibility fallback. |
| `66b1309` | Precomputed sync targets | Routing snapshots filter and sort each shard's targets once instead of allocating a target slice per key. |
| `0ef0207` | Scan-time sync snapshots | Page scans serialize the visited entry under one lock and avoid a second lookup for every key. |
| `7401e05` | Keyless replication values | `INTERNALSETV3` omits the duplicated key and stats, with automatic V2 binary and JSON fallback. |
| `c70d849` | Bounded target concurrency | Replication sends to four targets concurrently by default, preserves result order, and supports a serial setting of one. |
| `2747005` | Borrowed deferred batch payloads | Sync envelopes reuse their exclusively owned payload slice instead of copying the full request array. |
| `2b700e5` | Maintenance sync statistics | Anti-entropy sync no longer inflates client read or per-key hit statistics; explicit dumps retain read accounting. |
| `86fe5ca` | Fused HAT-trie iteration | One cgo call reads and advances each key/value pair, and direct Go string construction removes an intermediate byte allocation. |
| `f871c79` | Compact streaming replication protobuf | Sync groups retain compact key/binary records and stream chunked protowire encoding into gzip, materializing generic requests only for JSON or legacy fallback. |
| `69a6018` | Bounded gzip writer cache | Four gzip writers survive garbage collection, avoiding repeated compressor construction while bounding retained memory. |
| `471c229` | Skip unused sync payload estimates | Unlimited-size replication batches no longer calculate request-size estimates that will not be used. |
| `c1bf95a` | Persistent sync cursor | Ten-page scans reuse a generation-checked HAT-trie cursor and restart safely after mutation. |
| `a02c5a5` | Packed replication page arenas | Page keys and binary values share bounded byte arenas with compact indexes. |
| `5c6bd2f` | Native iterator batches | One cgo call returns up to 256 ordered trie records instead of crossing once per key. |
| `4c869d0` | Ordered gRPC replication stream | Anti-entropy sync can keep one acknowledged HTTP/2 stream per target, with configurable HTTP fallback. |
| `e5b127d` | Exact delta-first journal recovery | Retained deltas use one durable batch sync; compacted gaps fall back to an authenticated exact snapshot with stale-key deletion and restart-safe persistence. |
| `532270c` | Architectural bottleneck baselines | Reproducible Makefile/script benchmarks capture per-key memory, contended reads, durable writes, snapshot pause, anti-entropy, and command transport. |
| `3e79248` | Configurable per-key telemetry | Exact global counters remain while detailed key statistics support bounded/full modes and now default to off. |
| `6d148c2` | Concurrent scalar read fast path | In-memory scalar reads use shared locking and retain exclusive cleanup/hydration fallbacks for expiration and LevelDB. |
| `c549fb7` | Durable journal group commit | Concurrent mutators share bounded append and `fsync` batches without acknowledging or applying commands before durability succeeds. |
| `7f4c1e1` | Non-blocking snapshot output | Point-in-time capture remains consistent while encoding, compression, visitors, database diff, and output run after global locks are released. |
| `943adc2` | Incremental digest anti-entropy | Equal replicas exchange one root; mismatches merge bounded key-digest pages and transfer only changed/missing values and stale-key deletes. |
| `629ccca` | Persistent gRPC command stream | Ordered bidirectional commands share unary semantics while removing repeated RPC setup and supporting one-sender/one-receiver pipelining. |

## Operational Impact

- Replication can now be made durable, observable, and authenticated without
  changing the command API.
- LevelDB periodic sync has lower expected write amplification for mostly idle
  datasets because unchanged keys are skipped after the first full save.
- Benchmark evidence is easier to preserve and reproduce: `make bench-smoke`
  writes local artifacts, and `BENCHMARK.md` can be refreshed from artifacts
  instead of manually copying tables.
- The replication token is intentionally narrower than `MONITORING_AUTH_TOKEN`;
  it is not accepted for health, metrics, config, or normal client commands.
- Target-local replication batching reduces anti-entropy and async replay
  request fanout while keeping single-key replication compatible.
- Multi-node failure tests now prove bad replication credentials do not mutate
  followers and durable outbox jobs can replay after target recovery.
- Native batch transport removes repeated nested JSON encoding from the
  protobuf path while retaining backward compatibility for already serialized
  legacy batch payloads.
- Batch preflight changes failure semantics from partial apply to all-or-reject
  for invalid internal replication batches.
- Local benchmark evidence shows the 10k-key batched SyncAll path used 1
  request instead of 10,000 and completed about 335x faster than the unbatched
  path in the measured run.
- Multi-node chaos tests now exercise election-state changes and topology
  fingerprint mismatches through real HTTP monitoring servers.
- Borrowed envelopes, maintenance-only statistics, fused iteration, compact
  protowire streaming, and bounded gzip reuse reduce the 10k-key batched path
  to one request, about 0.95 MB of cumulative heap, about 30.2k allocations,
  and 55.8 KB of body bytes in the controlled median run.
- The latest pass is 1.51x faster, uses 6.10x less allocated heap, and performs
  1.68x fewer allocations than `84325af`; gzip compressor allocation alone is
  13.4x lower in the focused before/after profile.
- Four-target bounded delivery is 3.65x faster than serial delivery in the
  measured local latency benchmark, at about 1.15x cumulative heap and 12
  additional allocations per fanout.
- Small-set reads are 2.86x faster, plain-string radix scans are 2.02x faster,
  and priority-queue string push/pop is 1.14x faster in the focused medians.
- The current one-page 10k sync is 1.20x faster than `69a6018`, uses 1.12x
  less cumulative heap, and performs 2.95x fewer allocations.
- The default ten-page sync is 3.10x faster than the pre-cursor baseline, uses
  1.88x less cumulative heap, and performs 10.43x fewer allocations.
- Ordered gRPC sync is 1.19x faster than HTTP, sends 9.52% fewer bytes, and
  performs 24.41% fewer allocations, at 16.18% more cumulative heap.
- Batching 100 pulled journal records behind one `fsync` is 56.55x faster than
  syncing every command. Retained deltas are 42.70x faster than rebuilding the
  10k-key fixture and use 156.85x less cumulative heap.
- The final architecture pass bounds telemetry growth, makes contended scalar
  reads 2.42x faster, makes 16-caller durable writes 11.99x faster, and shortens
  the maximum snapshot reader pause by 3.71x.
- Equal-state digest anti-entropy is 6.99x faster, 49,971x smaller on wire, and
  206.1x lower in cumulative heap than a valid 10k-key full transfer. A 1%
  repair remains 2.13x faster and 44.75x smaller on wire.
- Sequential `CommandStream` is 3.96x faster than unary gRPC. Full-duplex
  pipelining is 18.94x faster, uses 7.67x less cumulative heap, and performs
  6.57x fewer allocations in the isolated 10,000-command medians.

## Final Architecture Measurements

All rows are medians on the same AMD Ryzen 9 5950X host. Detailed commands,
raw rows, and tradeoffs are in `BENCHMARK.md`.

| Feature | Baseline | Final | Improvement / tradeoff |
| --- | ---: | ---: | --- |
| Per-key telemetry, 100k keys | 242.5 retained B/key, unbounded | 63.57 retained B/key, off by default | 73.8% lower; opt-in 100k bound is 213.5 B/key |
| Concurrent scalar reads | 1,528 ns/read | 632.4 ns/read | 2.42x faster |
| Durable writes, 16 callers | 878,909 ns/write | 73,286 ns/write | 11.99x faster |
| Snapshot maximum read pause | 528,624,130 ns | 142,374,086 ns | 3.71x shorter; total snapshot time is 5.5% higher and heap is 2.63x higher |
| Equal anti-entropy, 10k x 1 KiB | 154,735,234 ns, 10,743,774 wire B | 22,129,470 ns, 215 wire B | 6.99x faster, 49,971x smaller wire, 206.1x lower heap |
| 1%-changed anti-entropy | Full-transfer baseline above | 72,812,784 ns, 240,086 wire B | 2.13x faster, 44.75x smaller wire, 11.47x lower heap |
| Sequential command transport, 10k | Unary: 59,040 ns/command | Stream: 14,914 ns/command | 3.96x faster, 6.73x lower heap |
| Pipelined command transport, 10k | Unary: 59,040 ns/command | Stream: 3,118 ns/command | 18.94x faster, 7.67x lower heap, 6.57x fewer allocations |

## Replication Batching Measurement

The current controlled comparison used 10,000 keys, `-benchtime=20x`, and
`-count=10` on the same AMD Ryzen 9 5950X host:

| Case | Time/op | Requests/op | Wire B/op | Heap B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before optimization (`b897b64`) | 162,195,812 ns | 1 | 144,227 | 57,035,706 | 1,040,310 |
| Start of this pass (`10cf4c8`) | 39,010,494 ns | 1 | 87,095 | 11,982,835 | 131,264 |
| Previous optimized (`c70d849`) | 31,830,774 ns | 1 | 55,792 | 5,668,513 | 50,753 |
| Start of latest pass (`84325af`) | 28,554,528 ns | 1 | 55,794 | 5,781,645 | 50,756 |
| Final optimized (`69a6018`) | 18,893,092 ns | 1 | 55,795 | 948,495 | 30,197 |
| Historical unbatched 10k | 51,455,645,995 ns | 10,000 | 2,135,564 | 1,794,046,848 | 202,050,916 |

The latest pass is 1.51x faster, uses 6.10x less allocated heap, and performs
1.68x fewer allocations than `84325af`, with body bytes unchanged. From
`10cf4c8`, the cumulative gains are 2.06x CPU, 1.56x body bytes, 12.63x
allocated heap, and 4.35x allocations. Against `b897b64`, the cumulative gains
are 8.58x CPU, 2.59x body bytes, 60.13x allocated heap, and 34.45x allocations.
The historical unbatched row remains as evidence of the original 10,000x
request reduction; it is not used to attribute the later routing and encoding
gains.

The gzip level benchmark kept BestSpeed as the default: default compression
used 2.39x fewer bytes on the repetitive fixture but required 4.15x CPU. The
bounded writer cache reduced sampled compressor allocation from 15.23 MB to
1.14 MB over 50 syncs, or 13.4x less compressor allocation.

The four-target latency benchmark used `-benchtime=20x -count=5`. Its medians
were 9,544,371 ns/op, 48,172 B/op, and 420 allocs/op in serial mode versus
2,617,552 ns/op, 55,269 B/op, and 432 allocs/op with a bound of four.

## Final Optimization Measurements

All rows below were measured on the same AMD Ryzen 9 5950X host. `B/op` is
cumulative allocated heap, not peak RSS.

| Replication sync case | Time/op | Wire B/op | B/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| One-page 10k at `69a6018` | 18,893,092 ns | 55,795 | 948,495 | 30,197 | Previous optimized baseline |
| One-page 10k at `e5b127d` | 15,698,676 ns | 55,794 | 847,763 | 10,241 | 1.20x CPU, 1.12x heap, 2.95x allocations |
| Default ten-page before cursor | 61,122,327 ns | unchanged | 1,877,005 | 123,996 | Traversal baseline |
| Default ten-page at `e5b127d` | 19,709,083 ns | 57,486 | 999,805 | 11,885 | 3.10x CPU, 1.88x heap, 10.43x allocations |

The page cursor is invalidated by trie generation changes, the page arena caps
eager allocation, and native iteration returns up to 256 records with a 4 KiB
key buffer. A 10,000-key scan therefore uses about 40 native batch calls rather
than roughly 10,000 per-record cgo crossings.

| 10k/ten-page transport | Time/op | Wire B/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| HTTP/protobuf | 44,957,163 ns | 57,479 | 19,652,940 | 123,772 |
| Ordered gRPC stream | 37,765,365 ns | 52,006 | 22,832,475 | 93,557 |

The gRPC path is opt-in with `REPLICATION_TRANSPORT=grpc-stream`; HTTP remains
the default and fallback. Set `REPLICATION_HTTP_FALLBACK=false` to fail closed.

| Journal recovery path | Work/op | Time/op | Wire B/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Retained delta, batched durability | 100 commands | 2,169,591 ns | 9,425 | 163,918 | 702 |
| Retained delta, per-command `fsync` control | 100 commands | 122,684,320 ns | 9,425 | 176,726 | 799 |
| Exact snapshot fallback | 10,000 keys | 92,648,544 ns | 56,267 | 25,709,960 | 231,154 |

The exact fallback is default-on only after a compacted gap. It streams a
source journal-barrier snapshot to an atomic, source-specific recovery file,
rejects regressing sequences before rename, replaces stale follower keys,
resets the local journal checkpoint, persists LevelDB when configured, and
advances the pull cursor last. Set
`JOURNAL_PULL_FULL_SYNC_FALLBACK=false` to retain fail-closed `409` behavior.

## Verification

Pre-change checks were run before each feature against the relevant existing
tests. Post-change checks included focused tests for each feature plus final
suite verification:

```sh
make run CMD='go test . -run "TestREADME|TestBenchmarkDocs|TestImprovementReport" -count=1 -v'
make run CMD='go test . -run TestCommandRequestBodyEncodesNativeBatchProtobuf -count=1 -v'
make run CMD='go test . -run TestMonitoringHandlerRejectsInvalidInternalReplicationBatchWithoutPartialMutation -count=1 -v'
make run CMD='go test . -run TestBenchmarkDocsListReplicationBatchingBenchmark -count=1 -v'
make run CMD='go test . -run NoTestsForBenchmark -bench BenchmarkHTTPReplicatorSyncAllBatching -benchmem -benchtime=1x'
make run CMD='go test -run=NONE -bench=BenchmarkHTTPReplicatorTargetFanout -benchmem -benchtime=20x -count=5'
make run CMD='go test ./internal/jsonwire -run=NONE -bench=BenchmarkGzipCompressionLevels -benchtime=20x -count=3 -benchmem'
make run CMD='go test ./cmd/hatrie-cache -run "TestRunReplicaAcceptsLeaderWriteAfterPrimaryMarkedOffline|TestRunRejectsReplicationWithStaleTopologyFingerprint" -count=1 -v'
make run CMD='go test . -run=NONE -bench="BenchmarkHTTPReplicatorSyncAllBatching/(Batched10k|Default1k)" -benchmem -benchtime=20x -count=7'
make run CMD='go test . -run=NONE -bench=BenchmarkReplicationSyncTransport -benchmem -benchtime=10x -count=7'
make bench-journal-catchup BENCHTIME=5x COUNT=7
make run CMD='go test ./...'
make verify-local
make verify-benchmark-md-update
make bench-smoke BENCH_SMOKE_ARTIFACT_DIR=/tmp/hatrie-bench-smoke-feature BENCH_SMOKE_RUN_ID=feature
```

All commits listed above were pushed to `master`.
