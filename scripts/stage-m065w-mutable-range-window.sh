#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  hat/hatSql/m065w_mutable_range_window.go \
  hat/hatSql/m065w_mutable_range_window_test.go \
  hat/hatSql/m065w_mutable_range_window_baseline_benchmark_test.go \
  scripts/benchmark-m065w-mutable-range-window.sh \
  scripts/format-m065w-mutable-range-window.sh \
  scripts/review-m065w-mutable-range-window.sh \
  scripts/stage-m065w-mutable-range-window.sh \
  scripts/commit-m065w-mutable-range-window.sh \
  scripts/push-m065w-mutable-range-window.sh \
  scripts/test-m065w-mutable-range-window.sh \
  scripts/verify-m065w-mutable-range-window.sh \
  INCREMENTAL_MUTABLE_RANGE_WINDOW.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md
git diff --cached --check
git diff --cached --stat
