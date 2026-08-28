#!/usr/bin/env sh
set -eu

git add Makefile SQL.md hat/hatSql/query.go hat/hatSql/slow_query.go hat/hatSql/slow_query_test.go scripts/audit-sql-catalog-goal.sh scripts/commit-sql-slow-query-samples.sh scripts/format-sql-slow-query-samples.sh scripts/review-sql-slow-query-samples.sh scripts/test-sql-slow-query-samples.sh
git commit -m 'feat: retain redacted SQL slow-query samples'
git push
