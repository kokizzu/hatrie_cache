#!/bin/sh
set -eu

git diff --check
git status --short
git diff -- Makefile scripts/audit-query-performance-goal.sh scripts/test-sql-index-maintenance.sh scripts/format-sql-index-maintenance.sh hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_index_maintenance_test.go
