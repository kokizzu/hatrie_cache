#!/bin/sh
set -eu

git diff --check -- \
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
git status --short -- \
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
