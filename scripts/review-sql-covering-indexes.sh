#!/bin/sh
set -eu

git diff --check
git status --short
git diff -- Makefile scripts/audit-query-performance-goal.sh scripts/test-sql-covering-indexes.sh scripts/format-sql-covering-indexes.sh hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_covering_index_test.go hat/hatSql/contracts.go hat/hatSql/query.go
