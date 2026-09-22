#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  M242_OPERATOR_METRICS.md \
  README.md \
  hat/hatSql/contracts.go \
  hat/hatSql/m242_operator_metrics_benchmark_test.go \
  hat/hatSql/m242_operator_metrics_current_benchmark_test.go \
  hat/hatSql/m242_operator_metrics_test.go \
  hat/hatSql/query.go \
  hat/hatSql/slow_query.go \
  scripts/benchmark-m242.sh \
  scripts/commit-m242.sh \
  scripts/format-m242.sh \
  scripts/push-m242.sh \
  scripts/race-m242.sh \
  scripts/stage-m242.sh \
  scripts/test-m242-package.sh \
  scripts/test-m242.sh \
  scripts/verify-docs-m242.sh \
  scripts/vet-m242.sh

git diff --cached --check
git status --short
