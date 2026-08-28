#!/bin/sh
set -eu

git add ./Makefile ./scripts/audit-sql-catalog-goal.sh ./scripts/test-sql-explain-format.sh ./scripts/format-sql-explain-format.sh ./scripts/review-sql-explain-format.sh ./scripts/commit-sql-explain-format.sh ./hat/hatSql/explain_format.go ./hat/hatSql/explain_format_test.go
git commit -m 'feat: render SQL explain plans as JSON and DOT'
git push
