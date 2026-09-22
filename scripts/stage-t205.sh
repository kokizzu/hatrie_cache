#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  T205_REPLICATION_PROGRESS_METRICS.md \
  hat/hatReplication/t205_replication_progress_metrics.go \
  hat/hatReplication/t205_replication_progress_metrics_test.go \
  hat/hatReplication/t205_replication_progress_metrics_benchmark_test.go \
  hat/hatReplication/t205_replication_progress_metrics_baseline_benchmark_test.go \
  scripts/benchmark-t205-baseline.sh \
  scripts/benchmark-t205.sh \
  scripts/commit-t205.sh \
  scripts/format-t205.sh \
  scripts/inspect-hatrie-tmp-recursive.sh \
  scripts/push-t205.sh \
  scripts/race-t205.sh \
  scripts/stage-t205.sh \
  scripts/test-t205-package.sh \
  scripts/test-t205.sh \
  scripts/verify-docs-t205.sh \
  scripts/vet-t205.sh
