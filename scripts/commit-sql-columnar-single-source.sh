#!/bin/sh
set -eu

git add -- Makefile hat/hatSql/query.go hat/hatSql/sql_columnar_single_source_test.go hat/hatCache/sql_columnar_generic_filter_benchmark_test.go scripts/format-sql-columnar-single-source.sh scripts/test-sql-columnar-single-source.sh scripts/benchmark-sql-columnar-single-source.sh scripts/review-sql-columnar-single-source.sh scripts/commit-sql-columnar-single-source.sh
git diff --cached --check
git commit -m "perf: remove maps from generic columnar filter rows"
git push origin master
