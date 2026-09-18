#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INSPIRATION.md \
  M032_DISTRIBUTED_FRONTIER.md \
  hat/hatSql/sql_distributed_frontier.go \
  hat/hatSql/sql_distributed_frontier_test.go \
  hat/hatSql/sql_distributed_frontier_benchmark_test.go \
  scripts/benchmark-m032-distributed-frontier.sh \
  scripts/format-m032-distributed-frontier.sh \
  scripts/race-m032-distributed-frontier.sh \
  scripts/test-m032-distributed-frontier.sh \
  scripts/verify-m032-distributed-frontier.sh \
  scripts/stage-m032-distributed-frontier.sh \
  scripts/commit-m032-distributed-frontier.sh \
  scripts/push-m032-distributed-frontier.sh \
  scripts/test-m032-sql-package.sh
