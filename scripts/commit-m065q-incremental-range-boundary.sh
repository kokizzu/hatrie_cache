#!/usr/bin/env bash
set -euo pipefail

git add Makefile INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md BENCHMARK.md INCREMENTAL_RANGE_WINDOW.md \
  hat/hatSql/incremental_range_boundary_window.go \
  hat/hatSql/m065q_incremental_range_boundary_benchmark_test.go \
  hat/hatSql/m065q_incremental_range_boundary_test.go \
  scripts/benchmark-m065q-incremental-range-boundary.sh \
  scripts/format-m065q-incremental-range-boundary.sh \
  scripts/test-m065q-incremental-range-boundary.sh \
  scripts/test-race-m065q-incremental-range-boundary.sh \
  scripts/vet-m065q-incremental-range-boundary.sh \
  scripts/review-m065q-incremental-range-boundary.sh \
  scripts/commit-m065q-incremental-range-boundary.sh \
  scripts/push-m065q-incremental-range-boundary.sh \
  scripts/inspect-sql-window-ideas-now.sh
git diff --cached --check
git commit -m 'add incremental range boundary windows'
