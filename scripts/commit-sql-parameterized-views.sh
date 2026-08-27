#!/bin/sh
set -eu

git add -- Makefile ./hat/hatSql/parameterized_view.go ./hat/hatSql/parameterized_view_test.go ./scripts/test-sql-parameterized-views.sh ./scripts/format-sql-parameterized-views.sh ./scripts/review-sql-parameterized-views.sh ./scripts/commit-sql-parameterized-views.sh
git commit -m "feat: add parameterized SQL views"
git push
