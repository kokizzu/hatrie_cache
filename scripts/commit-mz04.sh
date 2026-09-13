#!/usr/bin/env bash
set -eu

git diff --check
git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ04_FRONTIER_COMPACTION_METRICS.md \
  Makefile \
  README.md \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/mz04_compaction_metrics_benchmark_test.go \
  hat/hatPipeline/mz04_compaction_metrics_test.go \
  scripts/benchmark-mz04-after.sh \
  scripts/benchmark-mz04-before.sh \
  scripts/commit-mz04.sh \
  scripts/format-mz04.sh \
  scripts/push-mz04.sh \
  scripts/status-mz04.sh \
  scripts/test-mz04-full.sh \
  scripts/test-mz04-race.sh \
  scripts/test-mz04.sh
git diff --cached --check
git commit -m "feat: expose frontier compaction debt metrics"
