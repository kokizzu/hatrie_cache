#!/bin/sh
set -eu

git add -- Makefile ./hat/hatSql/query.go ./hat/hatSql/rewrite.go ./hat/hatSql/subquery.go ./hat/hatSql/aggregate_filter.go ./hat/hatSql/aggregate_filter_test.go ./scripts/test-sql-aggregate-filter.sh ./scripts/format-sql-aggregate-filter.sh ./scripts/review-sql-aggregate-filter.sh ./scripts/commit-sql-aggregate-filter.sh
git commit -m "feat: add SQL aggregate filters"
git push
