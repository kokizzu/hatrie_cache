#!/bin/sh
set -eu

git add ./Makefile ./scripts/audit-query-performance-goal.sh ./scripts/test-sql-columnar-scans.sh ./scripts/format-sql-columnar-scans.sh ./scripts/benchmark-sql-columnar-scans.sh ./scripts/review-sql-columnar-scans.sh ./scripts/commit-sql-columnar-scans.sh ./hat/hatSql/contracts.go ./hat/hatSql/query.go ./hat/hatCache/sql_query.go ./hat/hatCache/sql_columnar_scan_test.go ./hat/hatCache/sql_columnar_scan_benchmark_test.go
git commit -m 'feat: add SQL columnar scan path'
git push
