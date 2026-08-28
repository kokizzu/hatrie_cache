#!/usr/bin/env sh
set -eu

git add Makefile SQL.md hat/hatSql/index_hint.go hat/hatSql/index_hint_test.go hat/hatSql/query.go scripts/commit-sql-index-hints.sh scripts/format-sql-index-hints.sh scripts/review-sql-index-hints.sh scripts/test-sql-index-hints.sh
git commit -m 'feat: add diagnostic SQL index hints'
git push
