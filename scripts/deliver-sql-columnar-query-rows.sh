#!/bin/sh
set -eu

files='
BENCHMARK.md
Makefile
README.md
hat/hatSql/columnar_query_rows_benchmark_test.go
hat/hatSql/columnar_query_rows_test.go
hat/hatSql/query.go
scripts/benchmark-sql-query-rows-columnar.sh
scripts/deliver-sql-columnar-query-rows.sh
scripts/inspect-sql-columnar-scan.sh
scripts/profile-sql-query-rows-columnar.sh
scripts/test-sql-columnar-query-rows.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): stream columnar query rows'
git push
