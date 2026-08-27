#!/bin/sh
set -eu

git add \
  Makefile \
  scripts/audit-query-performance-goal.sh \
  scripts/test-sql-secondary-indexes.sh \
  scripts/format-sql-secondary-indexes.sh \
  scripts/review-sql-secondary-indexes.sh \
  scripts/commit-sql-secondary-indexes.sh \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_secondary_index_test.go
git diff --cached --check
git commit -m 'feat: combine SQL secondary indexes'
git push
