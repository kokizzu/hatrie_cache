#!/bin/sh
set -eu

git add -- Makefile ./hat/hatSql/query.go ./hat/hatSql/rewrite.go ./hat/hatSql/subquery.go ./hat/hatSql/correlated_subquery_test.go ./scripts/test-sql-correlated-subqueries.sh ./scripts/format-sql-correlated-subqueries.sh ./scripts/review-sql-correlated-subqueries.sh ./scripts/commit-sql-correlated-subqueries.sh
git commit -m "feat: add correlated SQL subqueries"
git push
