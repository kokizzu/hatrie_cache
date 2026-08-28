#!/bin/sh
set -eu

git diff --check
git diff --stat -- ./Makefile ./scripts/audit-query-performance-goal.sh ./scripts/test-sql-columnar-scans.sh ./scripts/format-sql-columnar-scans.sh ./scripts/benchmark-sql-columnar-scans.sh ./hat/hatSql/contracts.go ./hat/hatSql/query.go ./hat/hatCache/sql_query.go ./hat/hatCache/sql_columnar_scan_test.go ./hat/hatCache/sql_columnar_scan_benchmark_test.go
git status --short
