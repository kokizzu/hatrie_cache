#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C239_COMPACTION_METRICS.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatStorage/compaction_diagnostics.go \
  hat/hatStorage/compaction_metrics.go \
  hat/hatStorage/compaction_metrics_c239_benchmark_test.go \
  hat/hatStorage/m_u39_compaction_metrics_test.go \
  scripts/benchmark-ch239-compaction-metrics.sh \
  scripts/commit-ch239-compaction-metrics.sh \
  scripts/format-ch239-compaction-metrics.sh \
  scripts/push-ch239-compaction-metrics.sh \
  scripts/race-ch239-compaction-metrics.sh \
  scripts/stage-ch239-compaction-metrics.sh \
  scripts/test-ch239-compaction-metrics.sh \
  scripts/test-ch239-package.sh \
  scripts/vet-ch239-compaction-metrics.sh

git diff --cached --check
git diff --cached --stat
git status --short
