#!/bin/sh
set -eu

git diff --check -- Makefile ./hat/hatSql/query.go ./hat/hatSql/rewrite.go ./hat/hatSql/rewrite_test.go ./scripts/test-sql-rewrite.sh ./scripts/format-sql-rewrite.sh ./scripts/review-sql-rewrite.sh ./scripts/commit-sql-rewrite.sh
git status --short
