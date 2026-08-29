#!/bin/sh
set -eu

git add -- Makefile hat/hatSql/query.go hat/hatSql/regex.go hat/hatCache/sql_columnar_regexp_test.go hat/hatCache/sql_columnar_regexp_benchmark_test.go scripts/test-sql-columnar-regexp.sh scripts/format-sql-columnar-regexp.sh scripts/benchmark-sql-columnar-regexp.sh scripts/review-sql-columnar-regexp.sh scripts/commit-sql-columnar-regexp.sh
git diff --cached --check
git commit -m "perf: vectorize SQL columnar regexp filters"
git push origin master
