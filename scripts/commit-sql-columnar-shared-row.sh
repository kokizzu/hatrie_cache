#!/bin/sh
set -eu

git add -- Makefile hat/hatSql/query.go hat/hatSql/sql_columnar_shared_row_test.go scripts/test-sql-columnar-shared-row.sh scripts/format-sql-columnar-shared-row.sh scripts/review-sql-columnar-shared-row.sh scripts/commit-sql-columnar-shared-row.sh
git diff --cached --check
git commit -m "perf: share columnar rows in generic filters"
git push origin master
