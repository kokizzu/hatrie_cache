#!/usr/bin/env sh
set -eu

files='Makefile
hat/hatSql/query.go
hat/hatCache/sql_columnar_scan_test.go
hat/hatCache/sql_columnar_scan_benchmark_test.go
scripts/benchmark-sql-columnar-scan.sh
scripts/inspect-columnar-dispatch.sh
scripts/inspect-sql-columnar-tests.sh
scripts/inspect-sql-stream-aggregates.sh
scripts/format-sql-columnar-numeric-aggregate.sh
scripts/review-sql-columnar-numeric-aggregate.sh
scripts/commit-sql-columnar-numeric-aggregate.sh'

git add -- $files
git diff --cached --check
git commit --only -m 'perf: vectorize SQL columnar numeric aggregates' -- $files
git push origin master
