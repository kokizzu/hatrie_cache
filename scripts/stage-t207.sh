#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  INSPIRATION_ROUND2.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  T207_REPLICA_EVICTION_RECOVERY.md \
  hat/hatReplication/t206_replica_join_admission.go \
  hat/hatReplication/t207_replica_eviction_recovery.go \
  hat/hatReplication/t207_replica_eviction_recovery_test.go \
  hat/hatReplication/t207_replica_eviction_recovery_baseline_benchmark_test.go \
  hat/hatReplication/t207_replica_eviction_recovery_benchmark_test.go \
  scripts/test-t207.sh \
  scripts/benchmark-t207-baseline.sh \
  scripts/benchmark-t207.sh \
  scripts/race-t207.sh \
  scripts/vet-t207.sh \
  scripts/format-t207.sh \
  scripts/verify-docs-t207.sh \
  scripts/stage-t207.sh \
  scripts/commit-t207.sh \
  scripts/push-t207.sh

git diff --cached --check
