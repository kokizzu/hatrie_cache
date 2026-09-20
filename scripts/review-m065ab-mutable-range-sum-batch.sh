#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat -- \
  Makefile \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INCREMENTAL_MUTABLE_RANGE_WINDOW.md \
  hat/hatSql/m065w_mutable_range_window.go \
  hat/hatSql/m065w_mutable_range_window_test.go \
  hat/hatSql/m065w_mutable_range_window_baseline_benchmark_test.go \
  scripts/test-m065ab-mutable-range-sum-batch.sh \
  scripts/benchmark-m065ab-mutable-range-sum-batch.sh \
  scripts/format-m065ab-mutable-range-sum-batch.sh \
  scripts/verify-m065ab-mutable-range-sum-batch.sh \
  scripts/review-m065ab-mutable-range-sum-batch.sh \
  scripts/stage-m065ab-mutable-range-sum-batch.sh \
  scripts/commit-m065ab-mutable-range-sum-batch.sh \
  scripts/push-m065ab-mutable-range-sum-batch.sh
