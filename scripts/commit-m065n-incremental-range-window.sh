#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet --; then
  echo "refusing to commit because unrelated changes are already staged" >&2
  exit 1
fi

git add \
  Makefile \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  BENCHMARK.md \
  INCREMENTAL_RANGE_WINDOW.md \
  hat/hatSql/incremental_range_window.go \
  hat/hatSql/m065m_incremental_range_window_benchmark_test.go \
  hat/hatSql/m065n_incremental_range_window_test.go \
  scripts/format-m065m-incremental-range-window.sh \
  scripts/format-m065n-incremental-range-window.sh \
  scripts/test-m065n-incremental-range-window.sh \
  scripts/benchmark-m065n-incremental-range-window.sh \
  scripts/test-race-m065n-incremental-range-window.sh \
  scripts/vet-m065n-incremental-range-window.sh \
  scripts/review-m065n-incremental-range-window.sh \
  scripts/commit-m065n-incremental-range-window.sh \
  scripts/push-m065n-incremental-range-window.sh

git diff --cached --check
git commit -m "extend incremental range windows with extrema"
