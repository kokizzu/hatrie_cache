#!/bin/sh
set -eu

git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_distinct_in_benchmark_test.go hat/hatCache/sql_columnar_distinct_in_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-distinct-in.sh scripts/commit-sql-columnar-distinct-in.sh scripts/format-sql-columnar-distinct-in.sh scripts/stage-sql-columnar-distinct-in.sh scripts/test-sql-columnar-distinct-in.sh
git diff --cached --name-status
git diff --cached --check
