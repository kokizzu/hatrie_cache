#!/bin/sh
set -eu

git diff --check -- Makefile ./hat/hatSql/query.go ./hat/hatSql/regex.go ./hat/hatSql/regex_test.go ./scripts/test-sql-regex.sh ./scripts/format-sql-regex.sh ./scripts/review-sql-regex.sh ./scripts/commit-sql-regex.sh
git status --short
