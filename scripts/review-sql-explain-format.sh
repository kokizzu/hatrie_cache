#!/bin/sh
set -eu

git diff --check
git diff --stat -- ./Makefile ./scripts/audit-sql-catalog-goal.sh ./scripts/test-sql-explain-format.sh ./scripts/format-sql-explain-format.sh ./hat/hatSql/explain_format.go ./hat/hatSql/explain_format_test.go
git status --short
