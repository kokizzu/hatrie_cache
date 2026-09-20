#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  README.md \
  PRODUCT_IDEA_GAPS.md \
  BENCHMARK.md \
  TU26_INDEX_STRATEGY_INSPECTION.md \
  hat/hatSql/contracts.go \
  hat/hatSql/index_hint.go \
  hat/hatSql/index_strategy.go \
  hat/hatSql/query.go \
  hat/hatSql/tu26_index_strategy_test.go \
  hat/hatSql/tu26_index_strategy_benchmark_test.go \
  scripts/benchmark-tu26.sh \
  scripts/commit-tu26.sh \
  scripts/format-tu26.sh \
  scripts/push-tu26.sh \
  scripts/race-tu26.sh \
  scripts/review-tu26.sh \
  scripts/stage-tu26.sh \
  scripts/test-tu26-package.sh \
  scripts/test-tu26.sh \
  scripts/verify-tu26.sh \
  scripts/vet-tu26.sh
