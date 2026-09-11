#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  INCREMENTAL_RANGE_WINDOW.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  BENCHMARK.md \
  hat/hatSql/incremental_range_nth_value_window.go \
  hat/hatSql/m065r_incremental_range_nth_value_test.go \
  hat/hatSql/m065r_incremental_range_nth_value_benchmark_test.go \
  scripts/benchmark-m065r-incremental-range-nth-value.sh \
  scripts/format-m065r-incremental-range-nth-value.sh \
  scripts/test-m065r-incremental-range-nth-value.sh \
  scripts/test-race-m065r-incremental-range-nth-value.sh \
  scripts/vet-m065r-incremental-range-nth-value.sh \
  scripts/review-m065r-incremental-range-nth-value.sh \
  scripts/commit-m065r-incremental-range-nth-value.sh \
  scripts/push-m065r-incremental-range-nth-value.sh
git diff --cached --check
git commit -m 'add incremental range nth value'
