# Improvement Report

Date: 2026-07-17

## Scope

This report covers the reliability, observability, benchmark, and replication
auth improvements shipped in this work sequence.

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

## Verification

Pre-change checks were run before each feature against the relevant existing
tests. Post-change checks included focused tests for each feature plus final
suite verification:

```sh
make run CMD='go test ./...'
make verify-ci
make verify-benchmark-md-update
make bench-ci-smoke BENCH_CI_SMOKE_ARTIFACT_DIR=/tmp/hatrie-bench-ci-smoke-feature BENCH_CI_SMOKE_RUN_ID=feature
```

All commits listed above were pushed to `master`.
