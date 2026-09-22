#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M243_ARRANGEMENT_MEMORY_METRICS.md \
  README.md \
  Makefile \
  hat/hatSql/typed_table_arrangement_stats.go \
  hat/hatSql/m243_arrangement_memory_benchmark_test.go \
  hat/hatSql/m243_arrangement_memory_test.go \
  scripts/benchmark-m243.sh \
  scripts/commit-m243.sh \
  scripts/format-m243.sh \
  scripts/push-m243.sh \
  scripts/race-m243.sh \
  scripts/stage-m243.sh \
  scripts/test-m243-package.sh \
  scripts/test-m243.sh \
  scripts/verify-docs-m243.sh \
  scripts/vet-m243.sh

git diff --cached --check
git status --short
