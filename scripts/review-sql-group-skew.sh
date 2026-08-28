#!/bin/sh
set -eu

git diff --check
git diff --stat -- ./Makefile ./scripts/audit-query-performance-goal.sh ./scripts/test-sql-group-skew.sh ./scripts/format-sql-group-skew.sh ./scripts/benchmark-sql-group-skew.sh ./hat/hatSql/query.go ./hat/hatCache/sql_group_skew_test.go ./hat/hatCache/sql_group_skew_benchmark_test.go
git status --short
