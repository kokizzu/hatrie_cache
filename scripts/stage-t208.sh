#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  INSPIRATION_ROUND2.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  T208_ANONYMOUS_REPLICAS.md \
  hat/hatReplication/t206_replica_join_admission.go \
  hat/hatReplication/t207_replica_eviction_recovery.go \
  hat/hatReplication/write_quorum.go \
  hat/hatReplication/t208_anonymous_replica.go \
  hat/hatReplication/t208_anonymous_replica_test.go \
  hat/hatReplication/t208_anonymous_replica_baseline_benchmark_test.go \
  hat/hatReplication/t208_anonymous_replica_benchmark_test.go \
  scripts/test-t208.sh \
  scripts/benchmark-t208-baseline.sh \
  scripts/benchmark-t208.sh \
  scripts/format-t208.sh \
  scripts/race-t208.sh \
  scripts/vet-t208.sh \
  scripts/verify-docs-t208.sh \
  scripts/stage-t208.sh \
  scripts/commit-t208.sh \
  scripts/push-t208.sh

git diff --cached --check
