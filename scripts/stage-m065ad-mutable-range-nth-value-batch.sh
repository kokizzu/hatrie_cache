#!/usr/bin/env bash
set -eu

git add \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INCREMENTAL_MUTABLE_RANGE_NTH_VALUE_WINDOW.md \
  INSPIRATION.md \
  hat/hatSql/m065y_mutable_range_nth_value_window.go \
  hat/hatSql/m065y_mutable_range_nth_value_window_test.go \
  hat/hatSql/m065y_mutable_range_nth_value_window_benchmark_test.go \
  scripts/test-m065ad-mutable-range-nth-value-batch.sh \
  scripts/format-m065ad-mutable-range-nth-value-batch.sh \
  scripts/benchmark-m065ad-mutable-range-nth-value-batch.sh \
  scripts/verify-m065ad-mutable-range-nth-value-batch.sh \
  scripts/review-m065ad-mutable-range-nth-value-batch.sh \
  scripts/stage-m065ad-mutable-range-nth-value-batch.sh \
  scripts/commit-m065ad-mutable-range-nth-value-batch.sh \
  scripts/push-m065ad-mutable-range-nth-value-batch.sh
