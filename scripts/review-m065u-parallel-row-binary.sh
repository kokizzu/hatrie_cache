#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  Makefile \
  hat/hatSql/row_binary.go \
  hat/hatSql/row_binary_parallel_test.go \
  hat/hatSql/row_binary_parallel_benchmark_test.go \
  README.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  SQL_PARALLEL_ROW_BINARY.md \
  scripts/test-m065u-parallel-row-binary.sh \
  scripts/benchmark-m065u-parallel-row-binary-baseline.sh \
  scripts/benchmark-m065u-parallel-row-binary.sh \
  scripts/format-m065u-parallel-row-binary.sh \
  scripts/test-m065u-parallel-row-binary-full.sh \
  scripts/race-m065u-parallel-row-binary.sh \
  scripts/verify-m065u-parallel-row-binary-docs.sh \
  scripts/review-m065u-parallel-row-binary.sh \
  scripts/commit-m065u-parallel-row-binary.sh \
  scripts/push-m065u-parallel-row-binary.sh
git diff --stat -- \
  Makefile \
  hat/hatSql/row_binary.go \
  hat/hatSql/row_binary_parallel_test.go \
  hat/hatSql/row_binary_parallel_benchmark_test.go \
  README.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  SQL_PARALLEL_ROW_BINARY.md \
  scripts/test-m065u-parallel-row-binary.sh \
  scripts/benchmark-m065u-parallel-row-binary-baseline.sh \
  scripts/benchmark-m065u-parallel-row-binary.sh \
  scripts/format-m065u-parallel-row-binary.sh \
  scripts/test-m065u-parallel-row-binary-full.sh \
  scripts/race-m065u-parallel-row-binary.sh \
  scripts/verify-m065u-parallel-row-binary-docs.sh \
  scripts/review-m065u-parallel-row-binary.sh \
  scripts/commit-m065u-parallel-row-binary.sh \
  scripts/push-m065u-parallel-row-binary.sh
git status --short --untracked-files=all
