#!/usr/bin/env bash
set -eu

git add \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INCREMENTAL_MUTABLE_RANGE_BOUNDARY_WINDOW.md \
  INSPIRATION.md \
  hat/hatSql/m065x_mutable_range_boundary_window.go \
  hat/hatSql/m065x_mutable_range_boundary_window_test.go \
  hat/hatSql/m065x_mutable_range_boundary_window_benchmark_test.go \
  scripts/test-m065ac-mutable-range-boundary-batch.sh \
  scripts/format-m065ac-mutable-range-boundary-batch.sh \
  scripts/benchmark-m065ac-mutable-range-boundary-batch.sh \
  scripts/verify-m065ac-mutable-range-boundary-batch.sh \
  scripts/review-m065ac-mutable-range-boundary-batch.sh \
  scripts/stage-m065ac-mutable-range-boundary-batch.sh \
  scripts/commit-m065ac-mutable-range-boundary-batch.sh \
  scripts/push-m065ac-mutable-range-boundary-batch.sh
