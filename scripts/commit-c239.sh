#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C239_COMPACTION_METRICS.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatStorage/c239_compaction_metrics_benchmark_test.go \
  hat/hatStorage/c239_compaction_metrics_test.go \
  hat/hatStorage/compaction_diagnostics.go \
  hat/hatStorage/compaction_scheduler.go \
  hat/hatStorage/compaction_scheduler_stats.go \
  scripts/benchmark-c239.sh \
  scripts/commit-c239.sh \
  scripts/format-c239.sh \
  scripts/push-c239.sh \
  scripts/race-c239.sh \
  scripts/test-c239-package.sh \
  scripts/test-c239.sh \
  scripts/verify-c239-docs.sh \
  scripts/vet-c239.sh
git diff --cached --check
git commit -m "Add compaction backlog and amplification metrics"
