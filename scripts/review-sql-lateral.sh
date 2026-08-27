#!/bin/sh
set -eu

git diff --check -- Makefile ./hat/hatSql/query.go ./hat/hatSql/subquery.go ./hat/hatSql/lateral_test.go ./scripts/test-sql-lateral.sh ./scripts/format-sql-lateral.sh ./scripts/review-sql-lateral.sh ./scripts/commit-sql-lateral.sh
git status --short
