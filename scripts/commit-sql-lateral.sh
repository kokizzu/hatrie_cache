#!/bin/sh
set -eu

git add -- Makefile ./hat/hatSql/query.go ./hat/hatSql/subquery.go ./hat/hatSql/lateral_test.go ./scripts/test-sql-lateral.sh ./scripts/format-sql-lateral.sh ./scripts/review-sql-lateral.sh ./scripts/commit-sql-lateral.sh
git commit -m "feat: add SQL lateral joins"
git push
