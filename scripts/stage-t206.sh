#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  T206_REPLICA_BOOTSTRAP_JOIN.md \
  hat/hatReplication/t206_replica_join_admission.go \
  hat/hatReplication/t206_replica_join_admission_test.go \
  hat/hatReplication/t206_replica_join_admission_benchmark_test.go \
  scripts/benchmark-t206-baseline.sh \
  scripts/benchmark-t206.sh \
  scripts/commit-t206.sh \
  scripts/format-t206.sh \
  scripts/push-t206.sh \
  scripts/race-t206.sh \
  scripts/stage-t206.sh \
  scripts/test-t206-package.sh \
  scripts/test-t206.sh \
  scripts/verify-docs-t206.sh \
  scripts/vet-t206.sh
