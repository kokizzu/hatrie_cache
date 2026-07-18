# Improvement Report

Date: 2026-07-18

## Scope

This report covers the reliability, observability, benchmark, and replication
auth/transport improvements shipped in this work sequence. It also records the
follow-up batching, wire-format, and multi-node correctness hardening shipped
after the first report pass.

## Shipped Improvements

| Commit | Improvement | Result |
| --- | --- | --- |
| `aa54135` | Durable replication outbox | Async replication jobs and dead letters can survive process restart when `REPLICATION_OUTBOX_PATH` is configured. |
| `6c5dc30` | Dirty-key LevelDB sync | Periodic LevelDB persistence can write only changed keys after the initial full save, reducing repeated write work. |
| `9aff288` | Ops-state Prometheus metrics | Storage operations, replication queue state, retries, failures, and circuit-breaker state are visible in `/metrics`. |
| `f3dca88` | Multi-node replication integration test | Two real monitoring servers now verify end-to-end leader write replication. |
| `a2f93d9` | Benchmark artifact CI publishing | CI writes benchmark smoke JSON/Markdown/TSV/raw files and uploads them as workflow artifacts. |
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

## Operational Impact

- Replication can now be made durable, observable, and authenticated without
  changing the command API.
- LevelDB periodic sync has lower expected write amplification for mostly idle
  datasets because unchanged keys are skipped after the first full save.
- Benchmark evidence is easier to preserve and reproduce: CI uploads artifacts,
  and `BENCHMARK.md` can be refreshed from those artifacts instead of manually
  copying tables.
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
- Deferred metadata, precomputed routing, scan-time serialization, and keyless
  values reduce the 10k-key batched path to one request, about 5.67 MB of
  cumulative heap, about 50.8k allocations, and 55.8 KB of body bytes in the
  controlled median run.
- Four-target bounded delivery is 3.65x faster than serial delivery in the
  measured local latency benchmark, at about 1.15x cumulative heap and 12
  additional allocations per fanout.
- Small-set reads are 2.86x faster, plain-string radix scans are 2.02x faster,
  and priority-queue string push/pop is 1.14x faster in the focused medians.

## Replication Batching Measurement

The current controlled comparison used 10,000 keys, `-benchtime=20x`, and
`-count=10` on the same AMD Ryzen 9 5950X host:

| Case | Time/op | Requests/op | Wire B/op | Heap B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before optimization (`b897b64`) | 162,195,812 ns | 1 | 144,227 | 57,035,706 | 1,040,310 |
| Start of this pass (`10cf4c8`) | 39,010,494 ns | 1 | 87,095 | 11,982,835 | 131,264 |
| Current optimized (`c70d849`) | 31,830,774 ns | 1 | 55,792 | 5,668,513 | 50,753 |
| Historical unbatched 10k | 51,455,645,995 ns | 10,000 | 2,135,564 | 1,794,046,848 | 202,050,916 |

This pass is 1.23x faster, sends 1.56x fewer body bytes, uses 2.11x less
cumulative allocated heap, and performs 2.59x fewer allocations than its
starting point. Against `b897b64`, the cumulative gains are 5.10x CPU, 2.59x
body bytes, 10.06x allocated heap, and 20.50x allocations. The historical
unbatched row remains as evidence of the original 10,000x request reduction;
it is not used to attribute the later routing and encoding gains.

The four-target latency benchmark used `-benchtime=20x -count=5`. Its medians
were 9,544,371 ns/op, 48,172 B/op, and 420 allocs/op in serial mode versus
2,617,552 ns/op, 55,269 B/op, and 432 allocs/op with a bound of four.

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
make run CMD='go test ./cmd/hatrie-cache -run "TestRunReplicaAcceptsLeaderWriteAfterPrimaryMarkedOffline|TestRunRejectsReplicationWithStaleTopologyFingerprint" -count=1 -v'
make run CMD='go test ./...'
make verify-ci
make verify-benchmark-md-update
make bench-ci-smoke BENCH_CI_SMOKE_ARTIFACT_DIR=/tmp/hatrie-bench-ci-smoke-feature BENCH_CI_SMOKE_RUN_ID=feature
```

All commits listed above were pushed to `master`.
