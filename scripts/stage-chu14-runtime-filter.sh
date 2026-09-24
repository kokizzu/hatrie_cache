#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  SQL_RUNTIME_JOIN_FILTER.md \
  hat/hatSql/query.go \
  hat/hatSql/runtime_join_filter_benchmark_test.go \
  hat/hatSql/runtime_join_filter_test.go \
  scripts/benchmark-chu14-runtime-filter.sh \
  scripts/format-chu14-runtime-filter.sh \
  scripts/stage-chu14-runtime-filter.sh \
  scripts/test-chu14-runtime-filter.sh \
  scripts/test-chu14-sql-package.sh \
  scripts/race-chu14-sql-package.sh \
  scripts/vet-chu14-sql-package.sh \
  scripts/commit-chu14-runtime-filter.sh \
  scripts/push-chu14-runtime-filter.sh

git diff --cached --check
git diff --cached --stat
git diff --cached --name-only
