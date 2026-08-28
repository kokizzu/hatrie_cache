#!/bin/sh
set -eu

git add Makefile scripts/audit-query-performance-goal.sh scripts/test-sql-index-maintenance.sh scripts/format-sql-index-maintenance.sh scripts/review-sql-index-maintenance.sh scripts/commit-sql-index-maintenance.sh hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_index_maintenance_test.go
git commit -m 'feat: add SQL index maintenance queue'
git push
