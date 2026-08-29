#!/bin/sh
set -eu

git add -- Makefile hat/hatSql/query.go hat/hatCache/sql_columnar_like_test.go scripts/test-sql-columnar-like.sh scripts/format-sql-columnar-like.sh scripts/review-sql-columnar-like.sh scripts/commit-sql-columnar-like.sh
git diff --cached --check
git commit -m "perf: vectorize SQL columnar LIKE filters"
git push origin master
