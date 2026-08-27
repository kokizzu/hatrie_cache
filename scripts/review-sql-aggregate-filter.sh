#!/bin/sh
set -eu

git diff --check -- Makefile ./hat/hatSql/query.go ./hat/hatSql/rewrite.go ./hat/hatSql/subquery.go ./hat/hatSql/aggregate_filter.go ./hat/hatSql/aggregate_filter_test.go ./scripts/test-sql-aggregate-filter.sh ./scripts/format-sql-aggregate-filter.sh ./scripts/review-sql-aggregate-filter.sh ./scripts/commit-sql-aggregate-filter.sh
git status --short
