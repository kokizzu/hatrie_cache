#!/usr/bin/env sh
set -eu

files='Makefile
hat/hatSql/query.go
hat/hatCache/sql_columnar_scan_test.go
hat/hatCache/sql_columnar_scan_benchmark_test.go
scripts/benchmark-sql-columnar-stream-materialization.sh
scripts/format-sql-columnar-stream-materialization.sh
scripts/review-sql-columnar-stream-materialization.sh
scripts/commit-sql-columnar-stream-materialization.sh'

git add -- $files
git diff --cached --check
git commit --only -m 'perf: stream SQL columnar direct materialization' -- $files
git push origin master
