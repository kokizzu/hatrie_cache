# Improvement Report

Date: 2026-07-17

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

## Replication Batching Measurement

The focused local benchmark used 10,000 keys and `-benchtime=1x` to keep the
unbatched comparison bounded:

| Case | Time/op | Requests/op | Wire B/op | Heap B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Batched10k | 153,473,837 ns | 1 | 147,521 | 56,366,320 | 980,814 |
| Unbatched10k | 51,455,645,995 ns | 10,000 | 2,135,564 | 1,794,046,848 | 202,050,916 |

Measured deltas: about 335x faster, 10,000x fewer requests, 14.5x fewer body
bytes, 31.8x less heap, and 206x fewer allocations for the batched path.

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
make run CMD='go test ./cmd/hatrie-cache -run "TestRunReplicaAcceptsLeaderWriteAfterPrimaryMarkedOffline|TestRunRejectsReplicationWithStaleTopologyFingerprint" -count=1 -v'
make run CMD='go test ./...'
make verify-ci
make verify-benchmark-md-update
make bench-ci-smoke BENCH_CI_SMOKE_ARTIFACT_DIR=/tmp/hatrie-bench-ci-smoke-feature BENCH_CI_SMOKE_RUN_ID=feature
```

All commits listed above were pushed to `master`.
