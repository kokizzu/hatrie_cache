#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INSPIRATION.md \
  Makefile \
  README.md \
  SQL_SOURCE_FRONTIERS.md \
  hat/hatSql/sql_common_frontier_benchmark_test.go \
  hat/hatSql/sql_source_frontier.go \
  hat/hatSql/sql_source_frontier_test.go \
  scripts/benchmark-m032-frontier.sh \
  scripts/commit-m032-frontier.sh \
  scripts/format-m032-frontier.sh \
  scripts/push-m032-frontier.sh \
  scripts/review-m032-frontier.sh \
  scripts/test-m032-frontier.sh \
  scripts/test-race-m032-frontier.sh
git diff --cached --check
git commit -m "feat(sql): add indexed common source frontiers"
