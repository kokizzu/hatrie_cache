#!/bin/sh
set -eu

git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_sorted_projection_benchmark_test.go hat/hatCache/sql_columnar_sorted_projection_test.go hat/hatCache/sql_query.go hat/hatSql/contracts.go hat/hatSql/query.go scripts/benchmark-sql-columnar-sorted-projection.sh scripts/commit-sql-columnar-sorted-projection.sh scripts/format-sql-columnar-sorted-projection.sh scripts/stage-sql-columnar-sorted-projection.sh scripts/test-sql-columnar-sorted-projection.sh
git diff --cached --name-status
git diff --cached --check
