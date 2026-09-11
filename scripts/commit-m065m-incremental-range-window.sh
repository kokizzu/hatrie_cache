#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet --; then
  echo "refusing to commit because unrelated changes are already staged" >&2
  exit 1
fi

git add \
  Makefile \
  README.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  BENCHMARK.md \
  INCREMENTAL_RANGE_WINDOW.md \
  hat/hatSql/incremental_range_window.go \
  hat/hatSql/m065m_incremental_range_window_test.go \
  hat/hatSql/m065m_incremental_range_window_benchmark_test.go \
  scripts/audit-inspiration-now.sh \
  scripts/benchmark-m065m-incremental-range-window.sh \
  scripts/test-m065m-incremental-range-window.sh \
  scripts/format-m065m-incremental-range-window.sh \
  scripts/test-race-m065m-incremental-range-window.sh \
  scripts/vet-m065m-incremental-range-window.sh \
  scripts/review-m065m-incremental-range-window.sh \
  scripts/commit-m065m-incremental-range-window.sh \
  scripts/push-m065m-incremental-range-window.sh

git diff --cached --check
git commit -m "add peer-aware incremental range windows"
