#!/bin/sh
set -eu

git diff --check -- Makefile ./hat/hatSql/parameterized_view.go ./hat/hatSql/parameterized_view_test.go ./scripts/test-sql-parameterized-views.sh ./scripts/format-sql-parameterized-views.sh ./scripts/review-sql-parameterized-views.sh ./scripts/commit-sql-parameterized-views.sh
git status --short
