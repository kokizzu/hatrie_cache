#!/usr/bin/env sh
set -eu

git add Makefile SQL.md hat/hatSql/null_semantics_test.go hat/hatSql/query.go scripts/commit-sql-null-semantics.sh scripts/format-sql-null-semantics.sh scripts/review-sql-null-semantics.sh scripts/test-sql-null-semantics.sh
git commit -m 'feat: add SQL null semantics functions'
git push
