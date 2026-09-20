#!/usr/bin/env bash
set -eu

git add \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INCREMENTAL_MUTABLE_RANGE_WINDOW.md \
  INSPIRATION.md \
  hat/hatSql/m065w_mutable_range_window.go \
  hat/hatSql/m065w_mutable_range_window_test.go \
  hat/hatSql/m065w_mutable_range_window_baseline_benchmark_test.go \
  scripts/test-m065ae-mutable-range-extrema-batch.sh \
  scripts/format-m065ae-mutable-range-extrema-batch.sh \
  scripts/benchmark-m065ae-mutable-range-extrema-batch.sh \
  scripts/verify-m065ae-mutable-range-extrema-batch.sh \
  scripts/review-m065ae-mutable-range-extrema-batch.sh \
  scripts/stage-m065ae-mutable-range-extrema-batch.sh \
  scripts/commit-m065ae-mutable-range-extrema-batch.sh \
  scripts/push-m065ae-mutable-range-extrema-batch.sh
