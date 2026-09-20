#!/usr/bin/env bash
set -eu

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INCREMENTAL_MUTABLE_RANGE_WINDOW.md \
  INSPIRATION.md \
  Makefile \
  hat/hatSql/m065w_mutable_range_window.go \
  hat/hatSql/m065w_mutable_range_window_test.go \
  hat/hatSql/m065w_mutable_range_window_baseline_benchmark_test.go \
  scripts/test-m065af-mutable-range-aggregate-batch.sh \
  scripts/format-m065af-mutable-range-aggregate-batch.sh \
  scripts/benchmark-m065af-mutable-range-aggregate-batch.sh \
  scripts/verify-m065af-mutable-range-aggregate-batch.sh \
  scripts/review-m065af-mutable-range-aggregate-batch.sh \
  scripts/stage-m065af-mutable-range-aggregate-batch.sh \
  scripts/commit-m065af-mutable-range-aggregate-batch.sh \
  scripts/push-m065af-mutable-range-aggregate-batch.sh
git diff --cached --check
