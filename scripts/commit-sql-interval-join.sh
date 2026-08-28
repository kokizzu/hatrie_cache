#!/usr/bin/env sh
set -eu

make review-sql-interval-join
git add -- Makefile hat/hatSql/interval_join.go hat/hatSql/interval_join_test.go scripts/inspect-temporal-analytics-goal.sh scripts/test-sql-approximate-aggregates.sh scripts/test-sql-interval-join.sh scripts/format-sql-interval-join.sh scripts/review-sql-interval-join.sh scripts/commit-sql-interval-join.sh
git diff --cached --check
git commit --only -m 'feat: add SQL interval range joins' -- Makefile hat/hatSql/interval_join.go hat/hatSql/interval_join_test.go scripts/inspect-temporal-analytics-goal.sh scripts/test-sql-approximate-aggregates.sh scripts/test-sql-interval-join.sh scripts/format-sql-interval-join.sh scripts/review-sql-interval-join.sh scripts/commit-sql-interval-join.sh
git push
