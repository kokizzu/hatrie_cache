#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INCREMENTAL_RANK_WINDOW.md \
  INSPIRATION.md \
  Makefile \
  README.md \
  hat/hatSql/incremental_rank_window.go \
  hat/hatSql/m065_rank_window_benchmark_test.go \
  hat/hatSql/m065_rank_window_test.go \
  scripts/benchmark-m065-rank-window.sh \
  scripts/commit-m065-rank-window.sh \
  scripts/format-m065-rank-window.sh \
  scripts/push-m065-rank-window.sh \
  scripts/review-m065-rank-window.sh \
  scripts/test-m065-rank-window.sh
git diff --cached --check
git commit -m "feat(sql): add incremental rank window maintenance"
