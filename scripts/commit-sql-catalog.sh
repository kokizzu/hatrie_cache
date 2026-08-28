#!/usr/bin/env sh
set -eu

git add Makefile SQL.md hat/hatSql/catalog.go hat/hatSql/catalog_test.go hat/hatSql/query.go scripts/commit-sql-catalog.sh scripts/format-sql-catalog.sh scripts/review-sql-catalog.sh scripts/test-sql-catalog.sh
git commit -m 'feat: add SQL information schema catalog'
git push
